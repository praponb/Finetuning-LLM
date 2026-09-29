from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 130)
    .select("id", "order_id")
    .unionByName(df_order_items.filter(F.col("quantity") >= 5).select("id", "order_id"))
    .orderBy(F.col("order_id").desc())
)
