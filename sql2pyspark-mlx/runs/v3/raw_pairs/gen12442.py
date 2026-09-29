from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("city") == "Chiang Mai") & F.upper("email").like("%AVI%"))
    .groupBy("city", "is_active")
    .agg(F.min("credit_limit").alias("min_credit_limit"))
    .filter(F.col("min_credit_limit") >= 7400)
)
