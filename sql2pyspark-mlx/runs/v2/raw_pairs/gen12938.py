from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.min("end_date").alias("min_end_date"),
        F.countDistinct("monthly_fee").alias("unique_statuss"),
    )
    .orderBy(F.col("unique_statuss").desc(), "plan")
    .limit(5)
)
