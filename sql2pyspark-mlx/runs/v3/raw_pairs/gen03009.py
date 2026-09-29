from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(12, 15) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "browser",
        "started_at",
        F.date_trunc("week", "started_at").alias("started_at_period"),
    )
)
