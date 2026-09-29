from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region")
w_recent = Window.orderBy("sqft", "id")
w4 = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-06-02"))) & F.col("sqft").between(6661, 7039))
    .select(
        "*",
        F.upper("store_name").alias("store_name_upper"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_clean"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.channel") == "web"), "left_anti")
)

valid = (
    kept
    .filter((F.col("opened_date") > F.lit("2023-03-23").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "store_name_upper",
        "region_clean",
        F.initcap(F.trim("city")).alias("city_title"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.amount") <= 710), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.first("region", ignorenulls=True).over(w_part).alias("first_known_region"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
        F.lead("sqft", 1).over(w_recent).alias("next_sqft"),
        F.avg("sqft").over(w4).alias("sqft_avg_5_prior"),
        F.sum("sqft").over(w5).alias("cumulative_sqft"),
    )
)

summary = (
    enriched
    .withColumn("next_sqft_tier", F.when(F.col("next_sqft") >= 14856, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["next_sqft_tier", "city"], ["next_sqft_tier"], []],
        "next_sqft_tier",
        "city",
    )
    .agg(
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
        F.round(F.sum("sqft_share") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("sqft_share_per_berlin"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    summary
    .select(
        "next_sqft_tier",
        "city",
        "tokyo_events",
        "sqft_share_per_berlin",
        "num_rows",
    )
)
