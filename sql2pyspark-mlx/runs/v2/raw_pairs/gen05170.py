from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "store_id",
        F.date_format("sale_date", "yyyy-MM").alias("sale_month"),
        F.year("sale_date").alias("sale_year"),
    )
    .agg(F.countDistinct("revenue").alias("unique_stores"))
    .filter(F.col("unique_stores") >= 1)
    .orderBy(F.col("unique_stores").desc())
)
