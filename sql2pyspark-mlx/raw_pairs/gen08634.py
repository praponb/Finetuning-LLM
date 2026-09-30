from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.upper(F.trim(F.col("p.product_name"))) == "WIRELESS MOUSE")
    .groupBy(F.col("p.category"), F.col("p.supplier_id"))
    .agg(
        F.countDistinct(F.col("ss.revenue")).alias("unique_stores"),
        F.min(F.col("ss.revenue")).alias("min_revenue"),
    )
    .orderBy("category")
)
