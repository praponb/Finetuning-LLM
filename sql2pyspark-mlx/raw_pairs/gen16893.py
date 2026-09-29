from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 37)
    .select("quantity")
    .intersect(df_order_items.filter(F.col("order_id") == 40).select("quantity"))
)
