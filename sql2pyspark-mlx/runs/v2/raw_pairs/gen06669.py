from pyspark.sql import functions as F

min_unit_price = df_order_items.filter(F.col("product_id").isin([26, 38])).agg(F.min("unit_price")).first()[0]

result = (
    df_order_items
    .filter(F.col("unit_price") == min_unit_price)
    .select("id", "unit_price", "product_id", "quantity")
    .orderBy("quantity")
)
