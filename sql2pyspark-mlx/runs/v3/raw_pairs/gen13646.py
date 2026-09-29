from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.cost") <= 570)
    .groupBy(F.col("p.supplier_id"))
    .agg(F.max(F.col("ss.revenue")).alias("max_revenue"))
    .orderBy("max_revenue", "supplier_id")
    .limit(3)
)
