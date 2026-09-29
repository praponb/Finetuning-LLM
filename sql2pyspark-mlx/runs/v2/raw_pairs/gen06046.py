from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter(F.col("order_id").isin([23, 36, 39]) & ~F.col("carrier").isin(["Flash", "DHL"]))
)

summary = (
    valid_shipments
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("shipped_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
    )
)

result = (
    summary
    .orderBy(F.col("pct_fedex").desc(), "shipped_month")
    .limit(10)
)
