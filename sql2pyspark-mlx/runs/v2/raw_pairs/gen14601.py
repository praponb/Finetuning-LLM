from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.sum(F.when(F.col("segment") == "Corporate", F.col("credit_limit")).otherwise(0)).alias("corporate_credit_limit"),
        (F.max("age") - F.min("age")).alias("age_range"),
    )
    .orderBy(F.col("corporate_credit_limit").desc())
)
