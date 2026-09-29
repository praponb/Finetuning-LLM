from pyspark.sql import functions as F

base = (
    df_sessions
    .filter((F.col("started_at") >= F.to_date(F.lit("2023-08-30"))) & F.col("browser").isNotNull())
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.substring("country", 1, 2).alias("country_prefix"),
        F.when(F.col("browser") == "Chrome", 1).otherwise(0).alias("is_chrome"),
    )
)

result = (
    base
    .groupBy("browser")
    .pivot("country", ["SG", "DE"])
    .agg(F.avg("is_chrome").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
