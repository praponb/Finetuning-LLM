from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "EXPIRED"]) & (F.col("plan") == "premium"))
    .select(
        "id",
        "plan",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
    .orderBy("plan", "id")
)
