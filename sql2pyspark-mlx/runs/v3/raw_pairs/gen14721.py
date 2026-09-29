from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"), "store_id")
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.min("sale_date").alias("first_sale_date"),
    )
    .filter(F.col("revenue_range") >= 58)
)
