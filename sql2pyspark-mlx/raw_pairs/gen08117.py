from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter(~F.col("name").like("%Emma%"))
    .select(
        "*",
        F.length("segment").alias("segment_length"),
        F.initcap(F.trim("country")).alias("country_title"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

result = (
    filtered
    .groupBy("segment", "city")
    .pivot("country", ["JP", "DE"])
    .agg(F.avg("credit_limit"))
)
