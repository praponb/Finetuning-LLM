from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(190, 220))
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .filter(F.col("quantity_range") > 21)
)
