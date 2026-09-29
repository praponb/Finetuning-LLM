from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") == 46) | F.col("age").between(40, 63))
    .agg(
        F.sum(F.when(F.col("country") == "TH", 1).otherwise(0)).alias("th_count"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
    .orderBy(F.col("th_count").desc())
)
