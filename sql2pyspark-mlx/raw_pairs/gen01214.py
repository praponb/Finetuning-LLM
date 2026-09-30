from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy(F.col("sqft").asc_nulls_last())

matched_keys = (
    df_stores
    .select("region", "city")
    .exceptAll(
        df_stores
        .filter(F.col("city").isin(["London", "Berlin"]))
        .select("region", "city")
    )
)

verified = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.quantity") != 193), "left_anti")
)

kept_keys = (
    kept
    .select("city")
    .intersect(
        df_stores
        .filter(F.col("sqft").between(9664, 11502))
        .select("city")
    )
)

verified_stores = (
    kept
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

scored = (
    verified_stores
    .filter(F.col("opened_date") <= "2024-09-01")
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
    .withColumn("sqft_dense_rank", F.dense_rank().over(w2))
    .withColumn("sqft_share_2", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
)

result = (
    scored
    .withColumn("sqft_dense_rank_tier", F.when(F.col("sqft_dense_rank") >= 8, "HIGH").otherwise("LOW"))
    .withColumn("sqft_share_2_tier", F.when(F.col("sqft_share_2") >= 0.8, "HIGH").otherwise("LOW"))
    .withColumn("week_start", F.date_trunc("week", "opened_date"))
    .groupingSets(
        [["sqft_dense_rank_tier", "sqft_share_2_tier", "week_start"], ["sqft_dense_rank_tier", "sqft_share_2_tier"], []],
        "sqft_dense_rank_tier",
        "sqft_share_2_tier",
        "week_start",
    )
    .agg(
        F.sum(F.when(F.col("region") == "North", F.col("sqft_dense_rank"))).alias("north_total_sqft_dense_rank"),
        F.round(F.avg("sqft_share"), 2).alias("avg_sqft_share"),
        F.percentile_approx("sqft_share_2", 0.95).alias("sqft_share_2_p95"),
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.count_if(F.col("region") == "North").alias("north_events"),
    )
)
