from pyspark.sql import functions as F

metrics_long = (
    df_orders
    .select(
        "id",
        "status",
        F.col("discount").alias("DISCOUNT"),
        F.col("amount").alias("AMOUNT_VAL"),
        F.col("shipping_fee").alias("SHIPPING_FEE"),
    )
    .unpivot(
        ["id", "status"],
        ["DISCOUNT", "AMOUNT_VAL", "SHIPPING_FEE"],
        "measure",
        "metric_value",
    )
)

result = (
    metrics_long
    .groupBy(
        F.when(F.col("metric_value") >= 85, "HIGH").otherwise("LOW").alias("metric_value_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_rows"),
        F.round(F.avg("metric_value"), 2).alias("avg_metric_value"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("num_status") < 1)
)
