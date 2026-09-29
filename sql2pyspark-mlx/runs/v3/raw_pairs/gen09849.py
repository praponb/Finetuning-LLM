from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") != 5)
    .select("product_id", F.round("quantity", 2).alias("quantity_rounded"))
    .orderBy(F.col("quantity_rounded").desc(), "product_id")
)
