from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy("release_date_month_start", "id")

valid = (
    df_tracks
    .filter(F.col("duration_sec") >= 165)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.trunc(F.col("release_date"), "MM").alias("release_date_month_start"),
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
        F.when(F.col("duration_sec") >= 353, "HIGH").when(F.col("duration_sec") >= 183, "MEDIUM").otherwise("LOW").alias("duration_sec_tier"),
    )
)

latest_per_key = (
    valid
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .groupBy("release_date_quarter", "is_explicit")
    .pivot("duration_sec_tier", ["MEDIUM", "HIGH"])
    .agg(F.max("duration_sec"))
    .orderBy("release_date_quarter", "is_explicit")
)
