from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc())

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-06-21 00:00:00").cast("timestamp")) & (~F.col("browser").eqNullSafe(F.lit("Safari"))))
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.concat_ws("/", "device", "browser").alias("device_browser_key"),
        F.coalesce("browser", F.lit("N/A")).alias("browser_filled"),
    )
)

result = (
    valid
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)
