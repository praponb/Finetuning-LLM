from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter((F.col("signup_date") < "2023-06-27") & (F.col("signup_date") < "2023-08-01"))
    .withColumn("signup_date_minus_75d", F.date_sub("signup_date", 75))
)

summary = (
    valid_customers
    .groupBy("country")
    .agg(F.countDistinct("age").alias("unique_countrys"))
)

result = summary.orderBy(F.col("country").desc())
