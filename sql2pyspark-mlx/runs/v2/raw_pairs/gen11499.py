from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") > 3) & F.col("quantity").between(4, 8))
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .orderBy("quantity_range")
)
