from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_premium_tier = Window.partitionBy("premium_tier")
w_holder_id_by_premium_running = Window.partitionBy("holder_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.concat(F.col("product"), F.lit("#"), F.col("holder_id").cast("string")).alias("product_ref"),
        F.when(F.col("premium") >= 2844.1, "large").when(F.col("premium") >= 2096.6, "medium").otherwise("small").alias("premium_tier"),
    )
)

sequenced = (
    prepared
    .select(
        "*",
        F.count(F.lit(1)).over(w_premium_tier).alias("rows_in_premium_tier"),
        F.max("premium").over(w_premium_tier).alias("premium_tier_max_premium"),
        F.count(F.lit(1)).over(w_holder_id_by_premium_running).alias("running_count"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_premium_tier), F.lit(0)), 4).alias("premium_share_of_premium_tier"),
        F.round(F.col("premium") - F.avg("premium").over(w_premium_tier), 2).alias("premium_vs_premium_tier_avg"),
        F.sum("premium").over(w_premium_tier).alias("premium_tier_sum_premium"),
    )
)

status_stats = (
    sequenced
    .groupBy("status")
    .agg(F.percentile_approx("premium", 0.5).alias("median_premium"))
)

above_average = (
    sequenced.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.premium") >= F.col("g.median_premium"))
    .select(
        "s.*",
        "g.median_premium",
        F.round(F.col("s.premium") - F.col("g.median_premium"), 2).alias("premium_minus_median"),
    )
)

windowed = (
    above_average
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        "product_ref",
        "premium_tier",
        "rows_in_premium_tier",
        "running_count",
        "premium_share_of_premium_tier",
        "premium_vs_premium_tier_avg",
        "premium_tier_sum_premium",
        "median_premium",
        "premium_minus_median",
        F.round(F.avg("premium_tier_max_premium").over(w_premium_tier), 2).alias("premium_tier_avg_premium_tier_max_premium"),
        F.max("rows_in_premium_tier").over(w_premium_tier).alias("premium_tier_max_rows_in_premium_tier"),
    )
)

result = (
    windowed
    .rollup("premium_tier", "product", "status")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count(F.when(F.col("status").isin("EXPIRED", "ACTIVE", "CANCELLED"), 1)).alias("n_status_in_expired_active_can"),
        F.count("*").alias("cnt"),
        F.min("premium_tier_sum_premium").alias("min_premium_tier_sum_premium"),
    )
    .select(
        F.coalesce(F.col("premium_tier"), F.lit("ALL")).alias("premium_tier"),
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "grouping_level",
        "n_status_in_expired_active_can",
        "cnt",
        "min_premium_tier_sum_premium",
    )
    .orderBy(
        F.col("premium_tier").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
