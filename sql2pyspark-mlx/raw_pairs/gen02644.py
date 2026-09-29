from pyspark.sql import functions as F

valid_customers = df_customers.filter(F.col("segment").isin(["SMB", "Corporate"]))

agg = (
    valid_customers
    .groupBy("segment")
    .agg(
        F.sum(F.when(F.col("segment") == "Corporate", F.col("credit_limit")).otherwise(0)).alias("corporate_credit_limit"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
)

result = agg.select("segment", "corporate_credit_limit")
