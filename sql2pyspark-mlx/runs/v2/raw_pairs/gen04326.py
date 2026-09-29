from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "PAUSED"]))
    .withColumn("status_plan", F.concat("status", F.lit(" - "), "plan"))
    .orderBy(F.col("monthly_fee").desc())
)
