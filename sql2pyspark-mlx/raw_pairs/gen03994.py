from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 4, 9, 10]) & F.col("units").between(17, 25))
    .cube(
        F.date_format("sale_date", "yyyy-MM").alias("sale_month"),
        F.when(F.col("units") >= 35, "high").when(F.col("units") >= 12, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.sum("units").alias("total_units"),
        F.count("units").alias("units_count"),
    )
)
