from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_plan_by_birth_year_bucket_fu = Window.partitionBy("plan").orderBy("birth_year_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_plan_has_kpop = Window.partitionBy("plan", "has_kpop")
w_has_kpop_by_birth_year_bucke = Window.partitionBy("has_kpop").orderBy(F.col("birth_year_bucket").desc(), "id").rowsBetween(-2, Window.currentRow)
w_plan_by_birth_year_bucket_ti = Window.partitionBy("plan").orderBy("birth_year_bucket")

base_listeners = (
    df_listeners
    .filter((F.col("plan") == "premium") & (F.col("signup_date").isNotNull()))
    .select(
        "id",
        "username",
        "plan",
        "interests",
        F.array_contains(F.col("interests"), "kpop").alias("has_kpop"),
        F.lower(F.regexp_replace(F.trim(F.col("country")), " ", "_")).alias("country_slug"),
        (F.floor(F.col("birth_year") / 10) * 10).alias("birth_year_bucket"),
    )
)

sequenced = (
    base_listeners
    .select(
        "*",
        F.first("birth_year_bucket").over(w_plan_by_birth_year_bucket_fu).alias("first_birth_year_bucket"),
        F.round(F.col("birth_year_bucket") - F.avg("birth_year_bucket").over(w_plan_has_kpop), 2).alias("birth_year_bucket_vs_plan_has_kpop_avg"),
        F.round(F.avg("birth_year_bucket").over(w_has_kpop_by_birth_year_bucke), 2).alias("birth_year_bucket_avg_3"),
        F.nth_value("birth_year_bucket", 2).over(w_plan_by_birth_year_bucket_fu).alias("second_birth_year_bucket"),
        F.round(F.percent_rank().over(w_plan_by_birth_year_bucket_ti), 3).alias("birth_year_bucket_percent_rank"),
    )
)

exploded = (
    sequenced
    .select(
        "id",
        "username",
        "plan",
        "has_kpop",
        "country_slug",
        "birth_year_bucket",
        "first_birth_year_bucket",
        "birth_year_bucket_vs_plan_has_kpop_avg",
        "birth_year_bucket_avg_3",
        "second_birth_year_bucket",
        "birth_year_bucket_percent_rank",
        F.posexplode_outer("interests").alias("interest_pos", "interest"),
    )
)

result = (
    exploded
    .select(
        "id",
        "username",
        "plan",
        "has_kpop",
        "country_slug",
        "birth_year_bucket",
        "first_birth_year_bucket",
        "birth_year_bucket_vs_plan_has_kpop_avg",
        "birth_year_bucket_percent_rank",
        "interest_pos",
        "interest",
    )
    .orderBy(F.col("plan").desc())
)
