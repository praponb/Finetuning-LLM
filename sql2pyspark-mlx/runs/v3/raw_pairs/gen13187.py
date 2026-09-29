from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.countDistinct("age").alias("unique_segments"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("credit_limit")).otherwise(0)).alias("corporate_credit_limit"),
    )
)
