from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2024-03-01", "2024-07-09"))
    .agg(F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"))
)
