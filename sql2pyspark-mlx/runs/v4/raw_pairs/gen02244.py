from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee")
    .intersect(df_shipments.select(F.col("cost").alias("shipping_fee")))
)
