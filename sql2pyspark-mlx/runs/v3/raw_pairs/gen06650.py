from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(F.countDistinct("revenue").alias("unique_stores"))
    .filter(F.col("unique_stores") > 4)
)
