from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 41, "high").when(F.col("units") >= 19, "mid").otherwise("low").alias("units_bucket"),
        F.quarter("sale_date").alias("sale_quarter"),
    )
    .agg(
        F.avg("units").alias("avg_units"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
    .orderBy("sale_quarter")
)
