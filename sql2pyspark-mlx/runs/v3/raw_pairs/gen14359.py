from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["PAUSED", "EXPIRED", "ACTIVE"]) & (F.col("end_date") < "2025-08-01"))
    .select(
        "id",
        "end_date",
        F.quarter("start_date").alias("start_date_quarter"),
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
    )
)
