from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "outer")
    .filter(F.year(F.col("ss.sale_date")) == 2023)
    .select(F.col("ss.id"), F.col("ss.revenue"), F.col("s.region"), F.col("p.price"))
    .orderBy(F.col("price").desc())
)
