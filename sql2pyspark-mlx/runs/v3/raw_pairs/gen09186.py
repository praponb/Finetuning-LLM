from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(
        F.countDistinct("units").alias("unique_products"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .orderBy(F.col("avg_units").desc(), F.col("unique_products").desc())
)
