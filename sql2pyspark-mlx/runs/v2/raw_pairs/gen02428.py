from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("revenue") > 2800).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .filter(F.col("p.supplier_id").isNotNull())
    .select(F.col("p.id"), F.col("p.cost"), F.col("p.category"))
)
