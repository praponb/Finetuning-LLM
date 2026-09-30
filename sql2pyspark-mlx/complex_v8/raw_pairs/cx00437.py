from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
    )
)

labeled = (
    scoped
    .withColumn("move_type_slug", F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")))
)

qualified = (
    labeled
    .crossJoin(labeled.agg(F.avg("qty").alias("overall_avg_qty")))
    .filter(F.col("qty") > F.col("overall_avg_qty"))
    .drop("overall_avg_qty")
)

result = (
    qualified
    .groupingSets(
        [["sku_id", "operator"], ["sku_id"]],
        "sku_id", "operator",
    )
    .agg(
        F.grouping("sku_id").alias("is_total_sku_id"),
        F.round(F.stddev("qty"), 2).alias("stddev_qty"),
    )
    .orderBy(F.col("sku_id").asc_nulls_last(), F.col("operator").asc_nulls_last())
)
