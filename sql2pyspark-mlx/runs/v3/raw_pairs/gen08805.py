from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.array_contains("tags", "new")).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("ss.units") >= 12)
    .select(F.col("ss.id"), F.col("ss.sale_date"))
    .orderBy("id")
)
