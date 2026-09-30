from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") < 2800) | (F.col("sale_date") > "2023-04-02"))
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
)
