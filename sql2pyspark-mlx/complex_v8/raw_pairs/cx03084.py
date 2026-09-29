from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_group_plan_by_signup = Window.partitionBy("country_group", "plan").orderBy(F.col("signup_date").desc())
w_country_group_plan_by_signup_2 = Window.partitionBy("country_group", "plan").orderBy(F.col("signup_date").desc(), "id")
w_plan_country_group = Window.partitionBy("plan", "country_group")
w_country_group_plan = Window.partitionBy("country_group", "plan")
w_signup_date_month_plan = Window.partitionBy(F.date_trunc("month", "signup_date"), "plan")

cleaned = (
    df_listeners
    .filter(F.array_contains(F.col("interests"), "indie"))
    .select(
        "id",
        "username",
        "plan",
        "signup_date",
        "birth_year",
        "interests",
        F.when(F.col("country").isin("BR", "TH"), "group_a").when(F.col("country") == "KR", "group_b").otherwise("other").alias("country_group"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.round(F.cume_dist().over(w_country_group_plan_by_signup), 3).alias("signup_date_cume_dist"),
        F.row_number().over(w_country_group_plan_by_signup_2).alias("signup_date_seq"),
        F.round(F.avg("birth_year").over(w_plan_country_group), 2).alias("plan_country_group_avg_birth_year"),
        F.count(F.lit(1)).over(w_country_group_plan).alias("rows_in_country_group_plan"),
        F.size(F.collect_set("country_group").over(w_signup_date_month_plan)).alias("distinct_country_group_in_signup_date_month_plan"),
        F.round(F.col("birth_year") / F.nullif(F.sum("birth_year").over(w_country_group_plan), F.lit(0)), 4).alias("birth_year_share_of_country_group_plan"),
        F.lag("signup_date", 2).over(w_country_group_plan_by_signup_2).alias("prev2_signup_date"),
        F.lag("birth_year", 3).over(w_country_group_plan_by_signup_2).alias("prev3_birth_year"),
    )
)

result = (
    sequenced
    .filter(F.col("plan").isNotNull())
    .select(
        "id",
        "plan",
        "signup_date",
        "birth_year",
        "interests",
        "country_group",
        "signup_date_seq",
        "plan_country_group_avg_birth_year",
        "rows_in_country_group_plan",
        "prev2_signup_date",
        F.length(F.trim(F.col("username"))).alias("username_len"),
        F.length(F.trim(F.col("plan"))).alias("plan_len"),
    )
    .orderBy(F.col("rows_in_country_group_plan").desc_nulls_first())
)
