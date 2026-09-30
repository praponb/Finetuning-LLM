from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("start_date").between("2025-03-02", "2025-10-01") & F.col("customer_id").isNull())
    .withColumn("monthly_fee_level", F.when(F.col("monthly_fee") >= 194, "High").when(F.col("monthly_fee") >= 78, "Medium").otherwise("Low"))
)

summary = (
    src
    .groupBy("status")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
)

result = summary.select("status", "monthly_fee_count")
