from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~(((F.col("revenue") <= 2000) & (F.col("units") >= 19)) | F.col("product_id").isin([5, 6, 9])))
    .groupBy(F.year("sale_date").alias("sale_year"), "store_id")
    .agg(
        F.count("*").alias("total_count"),
        (F.max("units") - F.min("units")).alias("units_range"),
        F.sum("revenue").alias("total_revenue"),
    )
)
