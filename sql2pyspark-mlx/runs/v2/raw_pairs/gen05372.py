from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.avg("units").alias("avg_units"),
    )
    .filter(F.col("revenue_range") > 53)
    .select("product_id", "sale_month", "revenue_range", "avg_units")
    .orderBy("revenue_range", "product_id", "sale_month")
    .limit(5)
)
