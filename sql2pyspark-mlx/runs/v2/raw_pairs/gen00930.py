from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Bangkok", "Singapore", "Berlin"]))
    .select(
        "name",
        "email",
        F.upper(F.trim("name")).alias("name_normalized"),
        F.least("credit_limit", "age").alias("least_credit_limit_age"),
    )
)
