from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("ss.product_id").isin([1, 2, 4, 10]) & F.array_contains(F.col("p.tags"), "eco"))
    .select(F.col("p.brand"), F.col("ss.units"))
)
