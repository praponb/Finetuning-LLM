from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter((F.year(F.col("s.delivered_date")) == 2024) & F.col("o.amount").isNotNull())
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.delivered_date"),
        F.col("o.status"),
    )
    .orderBy(F.col("id").desc())
)
