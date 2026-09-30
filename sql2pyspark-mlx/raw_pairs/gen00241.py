from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 4, 6, 9]) & (F.col("revenue") <= 3000))
    .groupBy("product_id")
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.max("sale_date").alias("latest_sale_date"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .filter(F.col("revenue_range") > 59)
)
