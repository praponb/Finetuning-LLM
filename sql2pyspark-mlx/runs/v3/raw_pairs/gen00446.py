from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(5900, 9400) & (F.col("country").isin(["US", "UK", "DE"]) | F.col("name").like("%Taylor%")))
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.sum(F.when(F.col("country") == "US", 1).otherwise(0)).alias("us_count"),
        F.countDistinct("credit_limit").alias("unique_countrys"),
    )
)
