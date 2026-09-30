from pyspark.sql import functions as F

valid = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("city") == "Tokyo"))
    .select(
        "*",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("region", F.lit("NONE")).alias("region_clean"),
    )
)

result = (
    valid
    .groupBy("city")
    .pivot("region", ["West", "Central"])
    .agg(F.sum("sqft"))
)
