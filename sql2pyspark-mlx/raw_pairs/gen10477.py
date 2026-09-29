from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") <= 39) & (F.col("units") < 15))
    .withColumn("sale_date_label", F.date_format("sale_date", "MMM yyyy"))
    .withColumn("sale_date_period", F.date_trunc("YEAR", "sale_date"))
)
