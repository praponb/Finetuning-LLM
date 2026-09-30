from pyspark.sql import functions as F

max_unit_price = df_order_items.filter(F.col("quantity") == 7).agg(F.max("unit_price")).first()[0]

result = (
    df_order_items
    .filter(F.col("unit_price") >= max_unit_price)
    .select("id", "unit_price", "order_id")
    .orderBy(F.col("unit_price").desc(), "id")
    .limit(10)
)
