from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-05-30").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

result = (
    filtered
    .groupBy("city")
    .pivot("region", ["Central", "West"])
    .agg(F.sum("sqft").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
