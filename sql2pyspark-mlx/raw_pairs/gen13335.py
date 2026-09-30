from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .groupingSets(
        [["activity_month", "year_start"], ["activity_month"]],
        "activity_month",
        "year_start",
    )
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.avg("amount").alias("avg_amount"),
    )
)
