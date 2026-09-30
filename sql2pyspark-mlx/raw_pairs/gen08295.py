from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft")
w2 = Window.orderBy(F.col("sqft_rounded").desc())
w3 = Window.partitionBy(F.year("opened_date")).orderBy("sqft", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy(F.year("opened_date")).orderBy(F.col("opened_date").desc(), "id")
w5 = Window.orderBy("sqft_cume_dist", "id")

base = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-03-30").cast("date")) & ~F.col("city").isin(["London", "Chiang Mai", "Singapore"]))
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        F.round("sqft", 2).alias("sqft_rounded"),
    )
)

with_windows = (
    base
    .filter(F.length("store_name") < 12)
    .select(
        "id",
        "opened_date",
        "sqft",
        "sqft_rounded",
        F.cume_dist().over(w).alias("sqft_cume_dist"),
        F.percent_rank().over(w2).alias("sqft_rounded_percent_rank"),
        F.avg("sqft_rounded").over(w3).alias("sqft_rounded_avg_5"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "sqft",
        "sqft_cume_dist",
        "sqft_rounded_percent_rank",
        "sqft_rounded_avg_5",
        "event_seq",
        F.lead("sqft").over(w5).alias("next_sqft"),
        F.lag("sqft").over(w5).alias("prev_sqft"),
        F.lead("sqft_rounded_percent_rank").over(w5).alias("next_sqft_rounded_percent_rank"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

kept_keys = (
    scored
    .select("id")
    .subtract(
        df_stores
        .filter(F.col("region").isNull())
        .select("id")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys = (
    verified
    .select("id")
    .intersect(
        df_stores
        .filter(F.col("region").isin(["East", "West"]))
        .select("id")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("sqft_rounded_percent_rank") >= 0.79)
    .select(
        "id",
        "sqft_cume_dist",
        "sqft_rounded_percent_rank",
        "event_seq",
        "next_sqft",
        "prev_sqft",
        "next_sqft_rounded_percent_rank",
        "row_seq",
        F.when(F.col("event_seq") >= 8, "High").when(F.col("event_seq") >= 4, "Medium").otherwise("Low").alias("event_seq_level"),
    )
)

grouped = (
    cleaned
    .withColumn("next_sqft_tier", F.when(F.col("next_sqft") >= 15214, "LARGE").otherwise("SMALL"))
    .withColumn("sqft_rounded_percent_rank_tier", F.when(F.col("sqft_rounded_percent_rank") >= 0.76, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["next_sqft_tier", "sqft_rounded_percent_rank_tier"], ["next_sqft_tier"], []],
        "next_sqft_tier",
        "sqft_rounded_percent_rank_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("sqft_cume_dist").alias("max_sqft_cume_dist"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)

result = (
    grouped
    .select("next_sqft_tier", "sqft_rounded_percent_rank_tier", "distinct_ids")
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
