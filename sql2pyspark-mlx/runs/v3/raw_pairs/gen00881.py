from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([17, 30, 33, 38]))
    .select("product_id")
    .unionByName(df_order_items.filter(F.col("quantity") < 3).select("product_id"))
)
