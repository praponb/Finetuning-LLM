from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") < 6) | (F.col("quantity") <= 4))
    .select("id", "quantity", F.round("quantity", 2).alias("quantity_rounded"))
    .orderBy(F.col("quantity").desc(), F.col("id").desc())
)
