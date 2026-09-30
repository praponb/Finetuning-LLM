from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 5, 6]) & (F.col("revenue") < 1800))
    .groupBy(F.year("sale_date").alias("sale_year"), "product_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.max("sale_date").alias("latest_sale_date"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
    .orderBy(F.col("avg_revenue").desc(), "sale_year", "product_id")
    .limit(5)
)
