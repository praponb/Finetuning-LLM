from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date")
    .unionByName(df_shipments.select(F.col("shipped_date").alias("order_date")))
    .distinct()
)
