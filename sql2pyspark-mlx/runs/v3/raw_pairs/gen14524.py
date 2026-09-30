from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") != 6) | (F.col("unit_price") > 370))
    .select("id", "quantity", F.floor("quantity").alias("quantity_whole"))
    .orderBy(F.col("quantity").desc())
)
