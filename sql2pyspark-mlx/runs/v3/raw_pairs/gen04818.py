from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull() & (F.col("signup_date").between("2023-07-12", "2023-10-01") | (F.col("country") != "US")))
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
)
