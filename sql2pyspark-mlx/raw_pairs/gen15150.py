from pyspark.sql import functions as F

result = (
    df_products
    .select("cost")
    .union(df_order_items.select("unit_price"))
    .distinct()
    .orderBy(F.col("cost").desc())
)
