from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") == "Safari") & ~F.col("device").isin(["desktop", "mobile", "tablet"]) & F.hour("started_at").between(12, 17))
    .select("id", "started_at")
    .orderBy("started_at", "id")
    .limit(5)
)
