from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("opened_date"))
w2 = Window.orderBy(F.col("sqft").desc_nulls_first())

valid = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-03-22"))) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        F.when(F.col("sqft") >= 16234, "High").when(F.col("sqft") >= 4518, "Medium").otherwise("Low").alias("sqft_tier"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

remaining_keys = (
    valid
    .select("id")
    .subtract(
        df_stores
        .filter(F.lower("store_name").like("%ld %"))
        .select("id")
    )
)

verified = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") <= F.col("s.opened_date")), "left_anti")
)

scored = (
    kept
    .filter(F.col("store_name").isNotNull())
    .select(
        "store_name",
        "sqft",
        "sqft_tier",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.rank().over(w2).alias("sqft_rank"),
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share_2"),
        F.rank().over(w2).alias("sqft_rank_2"),
    )
)

cleaned = (
    scored
    .filter(F.col("sqft_share") <= 0.55)
    .select(
        "sqft_tier",
        "sqft_share_2",
        "sqft_rank_2",
        F.least("sqft_rank", "sqft").alias("least_sqft_rank_sqft"),
        F.floor("sqft").alias("sqft_whole"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("least_sqft_rank_sqft") >= 89, "PREMIUM").otherwise("STANDARD").alias("least_sqft_rank_sqft_tier"),
    )
    .agg(
        F.percentile_approx("sqft_share_2", [0.25, 0.5, 0.75]).alias("sqft_share_2_quartiles"),
        F.round(F.avg("sqft_whole"), 2).alias("avg_sqft_whole"),
        F.count("*").alias("row_count"),
        F.percentile_approx("sqft_share_2", [0.25, 0.5, 0.75]).alias("sqft_share_2_quartiles_2"),
        F.percentile_approx("least_sqft_rank_sqft", [0.25, 0.5, 0.75]).alias("least_sqft_rank_sqft_quartiles"),
        F.percentile_approx("sqft_share_2", 0.9).alias("sqft_share_2_p90"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy(F.col("sqft_share_2_p90").desc())
)
