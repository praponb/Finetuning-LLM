from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") > 220)
    .select("order_id")
    .unionByName(df_order_items.filter(F.col("quantity") < 5).select("order_id"))
    .distinct()
)
