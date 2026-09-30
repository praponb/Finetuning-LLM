from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.count("*").alias("num_customers"),
    )
    .orderBy(F.col("num_customers").desc())
)
