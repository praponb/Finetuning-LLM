from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(F.countDistinct("monthly_fee").alias("unique_statuss"))
    .filter(F.col("unique_statuss") >= 4)
    .select("status", "unique_statuss")
)
