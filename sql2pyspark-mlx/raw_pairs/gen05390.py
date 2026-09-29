from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 43, "high").when(F.col("units") >= 16, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .orderBy("min_sale_date")
)
