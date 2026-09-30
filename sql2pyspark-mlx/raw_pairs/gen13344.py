from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") <= 7)
    .select("quantity")
    .unionByName(df_order_items.filter(F.col("quantity").isNull()).select("quantity"))
    .orderBy("quantity")
)
