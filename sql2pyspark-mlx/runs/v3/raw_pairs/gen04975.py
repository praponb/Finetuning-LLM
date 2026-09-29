from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["standard", "enterprise"]))
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg(F.countDistinct("monthly_fee").alias("unique_statuss"))
    .filter(F.col("unique_statuss") >= 9)
    .select("end_quarter", "unique_statuss")
)
