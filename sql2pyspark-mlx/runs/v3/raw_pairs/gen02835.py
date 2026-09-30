from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 340)
    .select("product_id")
    .unionByName(df_order_items.filter(F.col("quantity") != 3).select("product_id"))
)
