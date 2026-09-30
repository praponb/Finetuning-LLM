from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .groupBy(
        F.date_format(F.col("products.launch_date"), "yyyy-MM").alias("launch_month"),
        F.col("products.brand"),
    )
    .agg(
        F.round(F.avg(F.col("store_sales.units")), 2).alias("avg_units"),
        F.avg(F.col("store_sales.revenue")).alias("avg_revenue"),
    )
    .orderBy("brand")
)
