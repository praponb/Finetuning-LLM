from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter(F.col("email").isNotNull() | F.col("email").like("%@startup.io"))
)

agg = (
    recent_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.min("age").alias("min_age"))
)

result = (
    agg
    .filter(F.col("min_age") < 58)
    .orderBy(F.col("min_age").desc(), F.col("signup_year").desc())
)
