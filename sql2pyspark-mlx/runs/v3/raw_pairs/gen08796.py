from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("carrier") != "UPS").alias("s"), F.col("o.id") == F.col("s.order_id"), "left_anti")
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
    )
    .orderBy(F.col("customer_id").desc())
)
