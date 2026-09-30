from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_plan_birth_year_tier = Window.partitionBy("plan", "birth_year_tier")
w_country_birth_year_tier_by_b = Window.partitionBy("country", "birth_year_tier").orderBy(F.col("birth_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_country_birth_year_tier_by_b_2 = Window.partitionBy("country", "birth_year_tier").orderBy(F.col("birth_year").desc())
w_country_birth_year_tier_by_b_3 = Window.partitionBy("country", "birth_year_tier").orderBy(F.col("birth_year").desc(), "id")

base_listeners = (
    df_listeners
    .filter((F.lower(F.col("username")).like("%no%")) & (F.col("signup_date") < F.lit("2025-05-05").cast("date")))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "birth_year",
    )
)

scored = (
    base_listeners
    .select(
        "*",
        F.when(F.col("birth_year") >= 1980, "large").when(F.col("birth_year") >= 1968, "medium").otherwise("small").alias("birth_year_tier"),
        F.concat(F.col("username"), F.lit("#"), F.col("id").cast("string")).alias("username_ref"),
    )
)

qualified = (
    scored.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.track_id") >= 24) & (F.hour(F.col("p.played_at")).between(6, 11)), "left_anti")
)

sequenced = (
    qualified
    .select(
        "id",
        "username",
        "country",
        "birth_year",
        "username_ref",
        F.max("birth_year").over(w_plan_birth_year_tier).alias("plan_birth_year_tier_max_birth_year"),
        F.last("birth_year", ignorenulls=True).over(w_country_birth_year_tier_by_b).alias("last_known_birth_year"),
        F.count(F.lit(1)).over(w_plan_birth_year_tier).alias("rows_in_plan_birth_year_tier"),
        F.rank().over(w_country_birth_year_tier_by_b_2).alias("birth_year_rank"),
        F.round(F.cume_dist().over(w_country_birth_year_tier_by_b_2), 3).alias("birth_year_cume_dist"),
        F.row_number().over(w_country_birth_year_tier_by_b_3).alias("rn"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "birth_year",
        "plan_birth_year_tier_max_birth_year",
        "rows_in_plan_birth_year_tier",
        "birth_year_rank",
    )
    .unpivot(
        ids=["id"],
        values=["birth_year", "plan_birth_year_tier_max_birth_year", "rows_in_plan_birth_year_tier", "birth_year_rank"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
