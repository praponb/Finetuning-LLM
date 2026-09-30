from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-10-07", "2024-12-26"))
    .select(F.lit("shipment").alias("origin"), "delivered_date")
    .union(df_orders.select(F.lit("order").alias("origin"), "order_date"))
    .distinct()
    .orderBy(F.col("delivered_date").desc())
)
