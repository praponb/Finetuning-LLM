from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~((F.col("unit_price") > 310) & (F.col("order_id") == 17)))
    .select("product_id", "quantity")
    .orderBy(F.col("product_id").desc())
)
