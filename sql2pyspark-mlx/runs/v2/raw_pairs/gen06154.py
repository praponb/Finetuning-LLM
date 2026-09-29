from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category"), F.col("p.brand"))
    .agg(
        F.min(F.col("ss.sale_date")).alias("first_sale_date"),
        F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"),
    )
    .filter(F.col("avg_revenue") >= 30)
    .orderBy("brand")
)
