from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.order_date").between("2024-02-22", "2025-02-01"))
    .groupBy(
        F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_month"),
        F.col("o.status"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.carrier") == "Kerry", 1).otherwise(0))) / F.count("*")), 1).alias("pct_kerry"),
    )
)
