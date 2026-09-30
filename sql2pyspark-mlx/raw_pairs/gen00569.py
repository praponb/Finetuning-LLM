from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") != "tablet") | (F.to_date("started_at") >= "2025-08-28"))
    .select(
        "id",
        "browser",
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
        (F.datediff(F.current_date(), "started_at") / 365).alias("years_since_started_at"),
    )
)
