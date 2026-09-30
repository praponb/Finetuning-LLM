from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(8, 14))
    .select(
        "browser",
        "customer_id",
        "device",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
        F.date_format("started_at", "MMM yyyy").alias("started_at_label"),
    )
)
