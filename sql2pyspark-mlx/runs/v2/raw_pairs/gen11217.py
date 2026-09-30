from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") <= 4)
    .select("product_id")
    .unionByName(df_order_items.filter(F.col("quantity") > 5).select("product_id"))
    .orderBy(F.col("product_id").desc())
)
