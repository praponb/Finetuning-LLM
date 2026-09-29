from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") > 1600)
    .groupBy(
        "product_id",
        F.year("sale_date").alias("sale_year"),
        F.month("sale_date").alias("sale_month"),
    )
    .agg(F.min("units").alias("min_units"))
    .orderBy(F.col("sale_month").desc())
)
