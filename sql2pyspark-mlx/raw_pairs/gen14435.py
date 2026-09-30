from pyspark.sql import functions as F

long_format = (
    df_orders
    .select("id", "customer_id", "discount", "amount", "shipping_fee")
    .unpivot(
        ids=["id", "customer_id"],
        values=["discount", "amount", "shipping_fee"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
)

result = (
    long_format
    .groupBy(
        "measure",
        F.when(F.col("metric_value") >= 79, "PREMIUM").otherwise("STANDARD").alias("metric_value_tier"),
    )
    .agg(
        F.count_if(F.col("measure") == "discount").alias("discount_events"),
        F.percentile_approx("metric_value", 0.5).alias("metric_value_p50"),
    )
    .filter(F.col("metric_value_p50") > 0)
    .orderBy(F.col("metric_value_p50").asc_nulls_first())
)
