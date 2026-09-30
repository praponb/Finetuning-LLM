from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .groupBy(
        F.quarter(F.col("products.launch_date")).alias("launch_quarter"),
        F.col("products.brand"),
    )
    .agg(F.round(F.avg(F.col("store_sales.revenue")), 2).alias("avg_revenue"))
    .orderBy(F.col("avg_revenue").desc(), F.col("launch_quarter").desc())
)
