from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(((F.col("device") != "tablet") & F.hour("started_at").between(6, 9)) | F.col("browser").isin(["Firefox", "Chrome"]))
    .select(
        "device",
        "customer_id",
        "country",
        F.length("browser").alias("browser_length"),
        F.initcap(F.trim("device")).alias("device_title"),
    )
    .orderBy(F.col("browser_length").desc(), "country")
)
