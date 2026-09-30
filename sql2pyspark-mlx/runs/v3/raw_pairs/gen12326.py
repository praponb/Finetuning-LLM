from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("credit_limit_plus_age", (F.col("credit_limit") + F.col("age")))
    .withColumn("country_group", F.when(F.col("country").isin(["TH", "DE"]), "Core").otherwise("Other"))
)
