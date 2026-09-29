from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 230)
    .withColumn("monthly_fee_filled", F.coalesce("monthly_fee", F.lit(0)))
)

summary = (
    src
    .groupBy(
        F.when(F.year("start_date") < 2024, "before_2024").when(F.year("start_date") == 2024, "2024").otherwise("after_2024").alias("start_cohort"),
    )
    .agg(
        F.max("start_date").alias("last_start_date"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
    )
)

result = summary
