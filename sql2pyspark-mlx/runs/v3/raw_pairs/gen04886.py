from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((~F.col("device").isin(["mobile", "desktop", "tablet"]) & ~F.col("browser").isin(["Chrome", "Safari", "Firefox"])) | ~F.col("browser").isin(["Firefox", "Edge", "Safari"]))
    .select(
        "id",
        "browser",
        F.trim("device").alias("clean_device"),
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
    )
    .orderBy("id")
)
