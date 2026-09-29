from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("monthly_fee") < 120) & (F.col("end_date") >= "2023-04-22"))
    .withColumn("clean_plan", F.trim("plan"))
)

agg = (
    base
    .groupBy(
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
        "status",
    )
    .agg(F.count("*").alias("num_subscriptions"))
)

result = (
    agg
    .filter(F.col("num_subscriptions") > 9)
    .select("end_cohort", "status")
    .orderBy("status")
)
