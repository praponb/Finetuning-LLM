from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.month("sale_date").alias("sale_month"),
        F.year("sale_date").alias("sale_year"),
    )
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.countDistinct("revenue").alias("unique_stores"),
    )
    .filter(F.col("units_range") >= 24)
    .orderBy(F.col("sale_month").desc())
)
