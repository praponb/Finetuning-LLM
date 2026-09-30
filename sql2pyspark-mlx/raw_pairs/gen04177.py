from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", "product_id")
    .agg(
        F.count("*").alias("row_count"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
    )
)
