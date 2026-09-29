from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([7, 28]))
    .select("quantity")
    .unionByName(df_order_items.filter(F.col("unit_price") < 280).select("quantity"))
)
