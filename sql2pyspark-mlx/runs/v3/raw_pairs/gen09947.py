from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select("order_id")
    .unionByName(df_order_items.filter(F.col("quantity") <= 3).select("order_id"))
    .orderBy(F.col("order_id").desc())
)
