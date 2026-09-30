from pyspark.sql import functions as F

metrics_long = (
    df_orders
    .select(
        "id",
        "customer_id",
        F.col("amount").alias("AMOUNT"),
        F.col("discount").alias("DISCOUNT_VAL"),
        "shipping_fee",
    )
    .unpivot(
        ids=["id", "customer_id"],
        values=["AMOUNT", "DISCOUNT_VAL", "shipping_fee"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)

result = (
    metrics_long
    .groupBy(
        "customer_id",
        F.when(F.col("metric_value") >= 74, "HIGH").otherwise("LOW").alias("metric_value_tier"),
    )
    .agg(
        F.min("metric_value").alias("min_metric_value"),
        F.countDistinct("metric_name").alias("distinct_metric_names"),
    )
)
