from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("credit_limit"))).alias("corporate_total_credit_limit"),
    )
    .filter(F.col("corr_credit_limit_age") >= 0)
    .orderBy(F.col("corr_credit_limit_age").desc())
)
