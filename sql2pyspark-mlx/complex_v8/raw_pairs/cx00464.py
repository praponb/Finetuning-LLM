from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_ids = Window.partitionBy("distinct_ids")
w_distinct_ids_by_distinct_ids = Window.partitionBy("distinct_ids").orderBy("distinct_ids", "country", "signup_date_plus_90d_month", "signup_date_plus_7d_ym").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_country_is_not_null_by_cnt = Window.partitionBy("n_country_is_not_null").orderBy(F.col("cnt").desc(), "country", "signup_date_plus_90d_month", "signup_date_plus_7d_ym")

scoped = (
    df_listeners
    .filter((F.col("signup_date").isNotNull()) & (F.col("plan") == "free"))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.date_add(F.col("signup_date"), 7).alias("signup_date_plus_7d"),
        F.date_add(F.col("signup_date"), 90).alias("signup_date_plus_90d"),
        F.concat(F.col("username"), F.lit("#"), F.col("id").cast("string")).alias("username_ref"),
    )
)

plan_stats = (
    scoped
    .groupBy("plan")
    .agg(F.max("birth_year").alias("max_birth_year"))
)

above_average = (
    scoped.alias("s")
    .join(plan_stats.alias("g"), F.col("g.plan") == F.col("s.plan"), "inner")
    .filter(F.col("s.birth_year") == F.col("g.max_birth_year"))
    .select(
        "s.*",
        "g.max_birth_year",
        F.round(F.col("s.birth_year") - F.col("g.max_birth_year"), 2).alias("birth_year_minus_max"),
    )
)

per_group = (
    above_average
    .filter(F.col("max_birth_year") < 1996.0)
    .groupBy(
        "country",
        F.date_trunc("month", "signup_date_plus_90d").alias("signup_date_plus_90d_month"),
        F.date_format("signup_date_plus_7d", "yyyy-MM").alias("signup_date_plus_7d_ym"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max("birth_year_minus_max").alias("max_birth_year_minus_max"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("country").isNotNull(), 1).otherwise(0)).alias("n_country_is_not_null"),
        F.round(F.avg("birth_year"), 2).alias("avg_birth_year"),
    )
)

with_windows = (
    per_group
    .select(
        "country",
        "signup_date_plus_90d_month",
        "signup_date_plus_7d_ym",
        "max_birth_year_minus_max",
        "distinct_ids",
        "n_country_is_not_null",
        "avg_birth_year",
        F.size(F.collect_set("country").over(w_distinct_ids)).alias("distinct_country_in_distinct_ids"),
        F.first("signup_date_plus_90d_month", ignorenulls=True).over(w_distinct_ids_by_distinct_ids).alias("first_known_signup_date_plus_90d_month"),
        (F.col("distinct_ids") - F.lag("distinct_ids").over(w_n_country_is_not_null_by_cnt)).alias("distinct_ids_change"),
    )
)

without_matches = (
    with_windows
    .crossJoin(with_windows.agg(F.max("distinct_ids").alias("overall_max_distinct_ids")))
    .filter(F.col("distinct_ids") < F.col("overall_max_distinct_ids"))
    .drop("overall_max_distinct_ids")
)

qualified = (
    without_matches
    .crossJoin(without_matches.agg(F.avg("distinct_ids").alias("overall_avg_distinct_ids")))
    .filter(F.col("distinct_ids") > F.col("overall_avg_distinct_ids"))
    .drop("overall_avg_distinct_ids")
)

result = (
    qualified
    .select(
        "country",
        "signup_date_plus_90d_month",
        "signup_date_plus_7d_ym",
        "max_birth_year_minus_max",
        "distinct_ids",
        "n_country_is_not_null",
        "avg_birth_year",
        "distinct_country_in_distinct_ids",
        "first_known_signup_date_plus_90d_month",
        "distinct_ids_change",
        F.concat(F.col("signup_date_plus_7d_ym"), F.lit("#"), F.col("n_country_is_not_null").cast("string")).alias("signup_date_plus_7d_ym_ref"),
    )
    .orderBy(F.col("avg_birth_year").desc_nulls_first(), F.col("distinct_ids").asc())
)
