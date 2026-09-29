from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Corporate", "SMB"]) & F.col("credit_limit").between(10500, 12400) & (F.col("country") != "SG"))
    .groupBy("country")
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.sum(F.when(F.col("country") == "UK", F.col("credit_limit")).otherwise(0)).alias("uk_credit_limit"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
)
