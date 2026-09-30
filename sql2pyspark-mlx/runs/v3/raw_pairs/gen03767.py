from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([2, 7, 11]))
    .groupBy("plan")
    .agg(
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"),
        F.countDistinct("monthly_fee").alias("unique_statuss"),
    )
    .filter(F.col("standard_count") < 8)
    .select("plan", "standard_count", "unique_statuss")
    .orderBy("plan", F.col("unique_statuss").desc())
)
