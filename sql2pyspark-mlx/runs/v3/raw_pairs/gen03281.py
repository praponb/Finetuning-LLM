from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter((F.col("ss.revenue") > 3200) & F.array_contains(F.col("p.tags"), "eco"))
    .select(F.col("p.launch_date"), F.col("ss.sale_date"))
)
