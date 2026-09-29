from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull() & F.col("email").like("%@startup.io"))
    .select("credit_limit")
    .orderBy(F.col("credit_limit").desc())
)
