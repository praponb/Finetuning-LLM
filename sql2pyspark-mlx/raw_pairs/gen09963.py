from pyspark.sql import functions as F

max_unit_price = df_order_items.filter(F.col("product_id").isNull()).agg(F.max("unit_price")).first()[0]

result = (
    df_order_items
    .filter(F.col("unit_price") > max_unit_price)
    .select("id", "unit_price", "quantity")
    .orderBy(F.col("unit_price").desc())
)
