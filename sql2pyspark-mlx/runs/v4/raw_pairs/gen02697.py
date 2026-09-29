from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-12-01", "2025-08-01") & (F.month("sale_date").isin([3, 6, 7]) | (F.col("units") >= 39)))
)

agg = (
    base
    .groupBy("store_id")
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.sum("revenue").alias("total_revenue"),
    )
)

result = (
    agg
    .select("store_id", "latest_sale_date", "total_revenue")
    .orderBy(F.col("store_id").desc(), F.col("total_revenue").desc())
)
