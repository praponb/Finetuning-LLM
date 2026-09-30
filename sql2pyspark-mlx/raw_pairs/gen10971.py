from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") < 200) & F.col("status").isin(["CANCELLED", "ACTIVE"]))
    .groupBy("status")
    .agg(F.count("*").alias("row_count"))
)
