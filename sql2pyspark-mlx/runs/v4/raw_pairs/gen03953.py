from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(8, 16) & (F.to_date("started_at") < "2025-09-01"))
    .select(
        "browser",
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
    )
    .orderBy("started_at_label")
)
