from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft_scaled", "id")
w2 = Window.orderBy("sqft_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region").orderBy(F.col("next_sqft_scaled").asc_nulls_first(), "id")

base = (
    df_stores
    .filter(F.col("opened_date") <= "2025-01-13")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.when(F.col("region").isin(["North", "Central"]), "Group A").otherwise("Other").alias("region_group"),
        F.concat_ws("-", "city", "region").alias("city_region_key"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.units").between(17, 31)), "left_semi")
)

not_flagged_stores = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.product_id") == 6), "left_semi")
)

base_stores = (
    not_flagged_stores
    .filter(F.col("sqft") < 15517)
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "region_group",
        "city_region_key",
        F.substring("region", 1, 2).alias("region_prefix"),
        F.round((F.col("sqft") * 1.1), 2).alias("sqft_scaled"),
    )
)

valid = (
    base_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "region_group",
        "city_region_key",
        "region_prefix",
        "sqft_scaled",
        F.upper("region").alias("region_upper"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

with_windows = (
    valid
    .withColumn("next_sqft_scaled", F.lead("sqft_scaled", 1).over(w))
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w2))
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy("region", "city")
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("next_sqft_scaled", [0.25, 0.5, 0.75]).alias("next_sqft_scaled_quartiles"),
        F.percentile_approx("sqft_scaled", [0.25, 0.5, 0.75]).alias("sqft_scaled_quartiles"),
        F.avg("sqft_scaled").alias("avg_sqft_scaled"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
    )
    .orderBy(F.col("event_count").desc_nulls_first())
)
