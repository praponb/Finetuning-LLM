from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~((F.col("start_date") <= "2024-10-01") | F.col("plan").isin(["basic", "enterprise"])))
    .select(
        "id",
        "plan",
        F.date_trunc("MONTH", "start_date").alias("start_date_period"),
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
    .orderBy(F.col("plan").desc())
)
