from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 2800)
    .withColumn("revenue_rounded", F.round("revenue", 2))
    .withColumn("sale_date_period", F.date_trunc("week", "sale_date"))
)
