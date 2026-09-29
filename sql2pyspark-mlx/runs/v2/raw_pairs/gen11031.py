from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(160, 210))
    .select("id", "quantity", F.floor("quantity").alias("quantity_whole"))
)
