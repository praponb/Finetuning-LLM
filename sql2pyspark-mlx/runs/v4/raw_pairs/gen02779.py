from pyspark.sql import functions as F

result = (
    df_order_items
    .select("unit_price")
    .unionByName(df_shipments.select(F.col("cost").alias("unit_price")))
)
