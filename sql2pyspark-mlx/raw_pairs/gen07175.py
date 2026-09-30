from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")

valid = (
    df_sessions
    .filter(F.col("country").isNotNull() & (F.col("started_at") >= F.to_timestamp(F.lit("2023-07-02 18:00:00"))))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.concat("country", F.lit(" - "), "device").alias("country_device"),
        F.coalesce("country", F.lit("NONE")).alias("resolved_country"),
    )
)

result = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "device", "started_at")
)
