from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(150, 180) & F.col("plan").isin(["enterprise", "basic", "premium"]))
    .select(
        "end_date",
        "plan",
        F.weekofyear("start_date").alias("start_date_weekofyear"),
    )
    .orderBy("end_date")
)
