from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([4, 15, 19, 20]) & (F.col("order_id").isin([6, 15, 26, 28]) | F.col("quantity").isNotNull()))
    .orderBy("product_id")
)
