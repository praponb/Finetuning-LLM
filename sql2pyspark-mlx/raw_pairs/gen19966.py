from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "UK") & (F.col("country") != "JP"))
    .withColumn("clean_email", F.trim("email"))
)
