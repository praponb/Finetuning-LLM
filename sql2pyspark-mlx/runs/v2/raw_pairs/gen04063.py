from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .groupBy(
        F.col("p.category"),
        F.date_trunc("MONTH", F.col("p.launch_date")).alias("launch_month"),
    )
    .agg(
        F.min(F.col("ss.revenue")).alias("min_revenue"),
        (F.max(F.col("ss.revenue")) - F.min(F.col("ss.revenue"))).alias("revenue_range"),
    )
    .orderBy("min_revenue")
)
