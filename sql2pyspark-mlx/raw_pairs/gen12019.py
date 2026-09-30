from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at_hour").asc_nulls_first(), "id")

cleaned = (
    df_sessions
    .filter((F.col("country").eqNullSafe(F.lit("TH"))) & F.col("customer_id").isin([3, 30]))
    .select(
        "*",
        F.hour("started_at").alias("started_at_hour"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_clean"),
    )
)

result = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "customer_id", "started_at_hour", "country_clean")
)
