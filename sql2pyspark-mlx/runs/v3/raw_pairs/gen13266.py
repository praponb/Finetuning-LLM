from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([7, 16, 18]))
    .select("quantity")
    .unionByName(df_order_items.filter(F.col("quantity") < 6).select("quantity"))
    .orderBy("quantity")
)
