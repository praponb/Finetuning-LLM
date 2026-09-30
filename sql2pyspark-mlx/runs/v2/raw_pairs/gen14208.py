from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") <= 4)
    .select("id", "unit_price", F.round("quantity", 1).alias("quantity_rounded"))
    .orderBy(F.col("unit_price").desc())
)
