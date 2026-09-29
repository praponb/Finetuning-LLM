from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(3, 7))
    .select("quantity")
    .unionByName(df_order_items.filter(F.col("quantity") == 6).select("quantity"))
)
