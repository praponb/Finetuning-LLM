from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-03-01 19:00:00").cast("timestamp")) & F.col("country").isin(["SG", "JP"]))
    .withColumn("started_at_day", F.col("started_at").cast("date"))
)

result = (
    valid
    .select("device")
    .intersectAll(
        df_sessions
        .filter(F.to_date("started_at") < "2024-07-03")
        .select("device")
    )
)
