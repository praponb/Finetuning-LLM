from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.min("sale_date").alias("min_sale_date"),
    )
    .filter(F.col("revenue_range") > 63)
    .select("store_id", "revenue_range", "min_sale_date")
    .orderBy("store_id", F.col("min_sale_date").desc())
)
