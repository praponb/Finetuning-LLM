from pyspark.sql import functions as F

valid = (
    df_policies
    .filter((F.col("start_date").isNotNull()) & (F.col("premium") > 2289.0))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

status_stats = (
    valid
    .groupBy("status")
    .agg(F.avg("premium").alias("avg_premium"))
)

above_average = (
    valid.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

qualified = (
    above_average.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status") == "OPEN") & (F.col("c.adjuster_id").isNotNull()), "left_semi")
)

labeled = (
    qualified
    .filter(F.col("premium_minus_avg") < 2500.0)
    .select(
        "*",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.concat_ws("-", F.col("status"), F.col("product")).alias("status_product_label"),
    )
)

result = (
    labeled
    .groupingSets(
        [["holder_id", "status"], ["holder_id"], ["status"]],
        "holder_id", "status",
    )
    .agg(
        F.round(F.stddev("premium_filled"), 2).alias("stddev_premium_filled"),
        F.sum("premium_filled").alias("total_premium_filled"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
    )
    .select(
        "holder_id",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "stddev_premium_filled",
        "total_premium_filled",
        "status_list",
    )
)
