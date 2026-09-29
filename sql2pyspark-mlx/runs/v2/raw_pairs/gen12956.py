from pyspark.sql import functions as F

result = (
    df_customers
    .cube("is_active", "country")
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("credit_limit")).otherwise(0)).alias("berlin_credit_limit"),
    )
)
