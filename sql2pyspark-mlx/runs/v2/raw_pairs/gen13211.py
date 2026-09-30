from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter((F.col("p.launch_date") < "2024-12-01") & F.col("ss.units").between(34, 40))
    .select(F.col("p.category"), F.col("ss.units"), F.col("ss.sale_date"))
    .orderBy(F.col("category").desc())
)
