from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(~F.col("status").isin(["PAUSED", "CANCELLED", "EXPIRED"]))
    .withColumn("monthly_fee_whole", F.floor("monthly_fee"))
)

end_quarter_summary = (
    recent_subscriptions
    .groupBy(
        F.quarter("end_date").alias("end_quarter"),
        F.date_format("start_date", "yyyy-MM").alias("start_month"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0)).alias("premium_count"),
    )
)

result = (
    end_quarter_summary
    .filter(F.col("premium_count") > 35)
    .select("end_quarter", "start_month", "premium_count")
    .orderBy(F.col("end_quarter").desc(), "start_month")
    .limit(3)
)
