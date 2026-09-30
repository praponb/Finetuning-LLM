from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(90, 180) & (F.year("end_date") >= 2023))
    .select(
        "id",
        "plan",
        "end_date",
        F.round((F.col("monthly_fee") * 1.07), 2).alias("adjusted_monthly_fee"),
    )
    .orderBy(F.col("adjusted_monthly_fee").desc(), F.col("plan").desc(), "id")
    .limit(5)
)
