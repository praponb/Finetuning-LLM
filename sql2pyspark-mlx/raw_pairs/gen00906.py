from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("sqft", "id")
w2 = Window.partitionBy(F.month("opened_date"), "city")
w3 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w_running = Window.partitionBy(F.month("opened_date"), "city").orderBy(F.col("sqft").asc_nulls_first())
w5 = Window.partitionBy("city")
w_running_2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running_3 = Window.orderBy("sqft_bucket", "id").rowsBetween(-2, -1)

filtered = (
    df_stores
    .filter(F.col("region").isin(["West", "North"]))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.when(F.col("region").isin(["West", "North"]), "Tier 1").otherwise("Other").alias("region_group"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_clean"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "region_clean",
        F.lead("sqft", 2).over(w).alias("next_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
        F.ntile(5).over(w3).alias("sqft_bucket"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "city",
        "sqft_bucket",
        F.dense_rank().over(w_running).alias("sqft_dense_rank"),
        F.avg("sqft").over(w5).alias("city_avg_sqft"),
        F.sum("sqft_bucket").over(w_running_2).alias("cumulative_sqft_bucket"),
        F.avg("next_sqft").over(w_running_3).alias("next_sqft_avg_2_prior"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (~F.col("c.status").isin(["PENDING", "CANCELLED"])), "left_semi")
)

not_flagged_stores = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.product_id").isin([2, 6])), "left_semi")
)

summary = (
    not_flagged_stores
    .withColumn("sqft_dense_rank_tier", F.when(F.col("sqft_dense_rank") >= 9, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["city", "sqft_dense_rank_tier"], ["city"]],
        "city",
        "sqft_dense_rank_tier",
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.min("sqft_dense_rank").alias("min_sqft_dense_rank"),
        F.count_if(F.col("city") == "Berlin").alias("berlin_events"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
    )
    .filter(F.col("berlin_events") > 1)
)

result = (
    summary
    .select(
        "city",
        "sqft_dense_rank_tier",
        "min_sqft_dense_rank",
        "num_cities",
        "bangkok_events",
    )
    .orderBy(F.col("bangkok_events").desc(), F.col("num_cities").desc())
)
