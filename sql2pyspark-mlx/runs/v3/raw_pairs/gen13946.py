from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") > 590) & (F.col("price") >= 410))
    .select("id", "product_name", "tags", "cost")
    .orderBy(F.col("id").desc())
)
