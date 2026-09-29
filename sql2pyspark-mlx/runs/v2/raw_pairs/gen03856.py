from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("age_band", F.when(F.col("age") >= 61, "High").when(F.col("age") >= 26, "Medium").otherwise("Low"))
    .orderBy("credit_limit")
)
