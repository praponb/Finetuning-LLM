from pyspark.sql import functions as F

max_unit_price = df_order_items.filter(F.col("order_id") == 16).agg(F.max("unit_price")).first()[0]

result = (
    df_order_items
    .filter(F.col("unit_price") > max_unit_price)
    .select("id", "unit_price", "product_id", "quantity")
    .orderBy(F.col("product_id").desc())
)
