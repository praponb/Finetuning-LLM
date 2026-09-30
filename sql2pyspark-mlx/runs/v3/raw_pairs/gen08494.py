from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("p.cost") <= 400)
    .groupBy(F.col("p.category"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.max(F.col("ss.revenue")).alias("max_revenue"),
        F.min(F.col("ss.sale_date")).alias("first_sale_date"),
    )
    .orderBy(F.col("max_revenue").desc(), "category")
    .limit(20)
)
