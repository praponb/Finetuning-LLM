from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Bangkok", "Singapore", "London"]))
    .agg(F.min("credit_limit").alias("min_credit_limit"))
    .orderBy(F.col("min_credit_limit").desc())
)
