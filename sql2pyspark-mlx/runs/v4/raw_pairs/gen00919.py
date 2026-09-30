from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") < 300)
    .select("order_id")
    .unionByName(df_order_items.filter(F.col("unit_price") >= 370).select("order_id"))
    .distinct()
    .orderBy("order_id")
)
