from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") == "TH") & ((F.col("device") == "mobile") | (F.col("country") == "TH")))
    .select(
        "id",
        "device",
        F.trim("browser").alias("clean_browser"),
        F.length("browser").alias("browser_length"),
    )
)
