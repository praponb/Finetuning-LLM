from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "customer_id").orderBy("started_at", "id")

filtered = (
    df_sessions
    .filter(F.col("country").isNotNull() & (~F.col("browser").eqNullSafe(F.lit("Firefox"))))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.when(F.col("device").isin(["tablet", "desktop"]), "Group A").otherwise("Other").alias("device_group"),
        F.lower("country").alias("country_lower"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "browser", "started_at", "country", "device_group")
)
