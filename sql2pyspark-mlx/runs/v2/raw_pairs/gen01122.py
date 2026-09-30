from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(F.col("product_id").isNotNull() & ((F.col("quantity") > 4) | (F.col("product_id") == 38))))
)
