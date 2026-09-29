from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("browser").isin(["Firefox", "Edge", "Safari"]))
    .select(
        "id",
        "started_at",
        F.date_trunc("year", "started_at").alias("started_at_period"),
        F.when(F.col("device") == "mobile", 1).otherwise(0).alias("is_mobile"),
    )
)
