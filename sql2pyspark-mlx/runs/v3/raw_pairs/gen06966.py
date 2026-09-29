from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id").isNull())
    .groupBy(F.quarter(F.col("p.launch_date")).alias("launch_quarter"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
    )
    .orderBy(F.col("total_revenue").desc(), F.col("cnt").desc())
)
