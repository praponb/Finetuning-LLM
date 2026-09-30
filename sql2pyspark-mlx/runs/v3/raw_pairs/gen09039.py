from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(((F.col("started_at") < "2023-10-23") & F.col("browser").isin(["Firefox", "Chrome", "Safari"])) | F.col("country").isin(["TH", "JP", "US"]))
    .select(
        "id",
        "device",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
    )
)
