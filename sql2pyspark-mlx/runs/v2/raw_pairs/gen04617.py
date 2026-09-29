from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter((F.year("shipped_date") == 2024) | (F.col("shipped_date") <= "2025-09-01"))
)

agg = (
    filtered_shipments
    .groupBy(
        "order_id",
        F.date_trunc("MONTH", "delivered_date").alias("delivered_month"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
    )
)

result = (
    agg
    .filter(F.col("pct_fedex") > 34)
    .select("order_id", "delivered_month", "pct_fedex")
    .orderBy("order_id")
)
