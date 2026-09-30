from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date", F.date_trunc("WEEK", "sale_date").alias("sale_date_period"))
    .orderBy(F.col("sale_date_period").desc())
)
