from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull() & (F.col("credit_limit") <= 4900))
    .withColumn("country_upper", F.upper("country"))
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 18000, "High").when(F.col("credit_limit") >= 6500, "Medium").otherwise("Low"))
)
