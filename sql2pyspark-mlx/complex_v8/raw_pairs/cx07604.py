from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_rows_by_max_rate_ties = Window.partitionBy("n_rows").orderBy(F.col("max_rate").asc_nulls_last())
w_n_rows_distinct_ids = Window.partitionBy("n_rows", "distinct_ids")

scoped_tariffs = (
    df_tariff_windows
    .filter((~F.col("tariff").isin("FLAT", "TOU")) & (F.col("rate") > 2.2))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.coalesce(F.col("rate"), F.lit(0.0)).alias("rate_filled"),
    )
)

enriched = (
    scoped_tariffs
    .select(
        "*",
        F.substring(F.trim(F.col("tariff")), 1, 4).alias("tariff_prefix"),
        F.when(F.col("rate_filled") >= 5.9, "HIGH").when(F.col("rate_filled") >= 4.1, "MEDIUM").otherwise("LOW").alias("rate_filled_tier"),
    )
)

per_group = (
    enriched
    .groupBy(F.date_trunc("month", "valid_to").alias("valid_to_month"))
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("rate").alias("max_rate"),
    )
)

filtered = (
    per_group
    .crossJoin(per_group.agg(F.max("distinct_ids").alias("overall_max_distinct_ids")))
    .filter(F.col("distinct_ids") < F.col("overall_max_distinct_ids"))
    .drop("overall_max_distinct_ids")
)

with_windows = (
    filtered
    .select(
        "valid_to_month",
        "n_rows",
        F.round(F.cume_dist().over(w_n_rows_by_max_rate_ties), 3).alias("max_rate_cume_dist"),
        F.round(F.col("max_rate") - F.avg("max_rate").over(w_n_rows_distinct_ids), 2).alias("max_rate_vs_n_rows_distinct_ids_avg"),
        F.count(F.lit(1)).over(w_n_rows_distinct_ids).alias("rows_in_n_rows_distinct_ids"),
    )
)

with_activity = (
    with_windows
    .crossJoin(with_windows.agg(F.avg("rows_in_n_rows_distinct_ids").alias("overall_avg_rows_in_n_rows_distinct_ids")))
    .filter(F.col("rows_in_n_rows_distinct_ids") > F.col("overall_avg_rows_in_n_rows_distinct_ids"))
    .drop("overall_avg_rows_in_n_rows_distinct_ids")
)

qualified = (
    with_activity
    .crossJoin(with_activity.agg(F.avg("max_rate_cume_dist").alias("overall_avg_max_rate_cume_dist")))
    .filter(F.col("max_rate_cume_dist") > F.col("overall_avg_max_rate_cume_dist"))
    .drop("overall_avg_max_rate_cume_dist")
)

result = (
    qualified
    .select(
        "valid_to_month",
        "n_rows",
        "max_rate_cume_dist",
        "max_rate_vs_n_rows_distinct_ids_avg",
        "rows_in_n_rows_distinct_ids",
        F.round(F.col("max_rate_cume_dist") / F.nullif(F.col("n_rows"), F.lit(0)), 3).alias("max_rate_cume_dist_per_n_rows"),
    )
    .orderBy(F.col("max_rate_cume_dist_per_n_rows").desc())
)
