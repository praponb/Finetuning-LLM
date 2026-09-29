from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("units") >= 17).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .filter(F.col("p.price") > 650)
    .select(F.col("p.id"), F.col("p.cost"), F.col("p.price"))
    .orderBy(F.col("price").desc())
)
