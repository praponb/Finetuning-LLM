from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_north", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("opened_date"), "city")
w_ordered = Window.partitionBy("city")
w_part = Window.partitionBy(F.date_trunc("YEAR", "opened_date"), "city")

filtered = (
    df_stores
    .filter(F.year("opened_date") >= 2023)
    .select("*", F.when(F.col("region") == "North", 1).otherwise(0).alias("is_north"))
)

with_windows = (
    filtered
    .filter(F.col("region") != "South")
    .withColumn("cumulative_sqft", F.sum("sqft").over(w))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4))
)

kept_keys = (
    with_windows
    .select("city", "region")
    .subtract(
        df_stores
        .filter(F.col("opened_date") < "2024-10-21")
        .select("city", "region")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("city").isin(["Tokyo", "Singapore"]))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        "is_north",
        "cumulative_sqft",
        "sqft_share",
        F.coalesce("cumulative_sqft", F.lit(0)).alias("cumulative_sqft_filled"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.max("cumulative_sqft").over(w_ordered).alias("city_max_cumulative_sqft"),
        F.sum("sqft").over(w_ordered).alias("city_sum_sqft"),
        F.avg("is_north").over(w_ordered).alias("city_avg_is_north"),
        F.min("is_north").over(w_part).alias("opened_date_period_min_is_north"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("cumulative_sqft") >= 72306, "LARGE").otherwise("SMALL").alias("cumulative_sqft_tier"),
        "city",
    )
    .agg(
        F.percentile_approx("is_north", 0.5).alias("is_north_p50"),
        F.percentile_approx("city_avg_is_north", [0.25, 0.5, 0.75]).alias("city_avg_is_north_quartiles"),
    )
    .orderBy(F.col("cumulative_sqft_tier").desc())
)
