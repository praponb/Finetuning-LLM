from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device") == "mobile")
    .select(
        "browser",
        F.initcap(F.trim("device")).alias("device_title"),
        F.initcap(F.trim("device")).alias("device_title_2"),
    )
    .orderBy(F.col("device_title_2").desc())
)
