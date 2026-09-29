from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(6, 8))
    .select("product_id", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)
