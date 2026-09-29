from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("category") == "Home").alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("ss.sale_date") <= "2024-08-01")
    .select(F.col("ss.id"), F.col("ss.store_id"))
    .orderBy("id")
    .limit(1)
)
