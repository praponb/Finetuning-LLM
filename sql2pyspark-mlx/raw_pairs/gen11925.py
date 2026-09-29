from pyspark.sql import functions as F

result = (
    df_products
    .select("price")
    .union(df_order_items.filter(F.col("quantity") > 4).select("unit_price"))
)
