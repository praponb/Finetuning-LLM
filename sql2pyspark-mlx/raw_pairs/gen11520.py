from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")
w2 = Window.partitionBy("city", "region")
w_running = Window.partitionBy(F.month("opened_date"), "region").orderBy("sqft", "id")
w_recent = Window.partitionBy("region").orderBy("prev_sqft", "id")
w5 = Window.orderBy("prev_sqft", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.orderBy(F.col("next_sqft").desc())

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull())
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
    .withColumn("resolved_region", F.coalesce("region", F.lit("N/A")))
)

enriched = (
    cleaned
    .filter(F.col("sqft") == 10130)
    .select(
        "*",
        F.lead("sqft").over(w).alias("next_sqft"),
        F.lag("sqft").over(w).alias("prev_sqft"),
        F.row_number().over(w).alias("seq_no"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "next_sqft",
        "seq_no",
        "sqft_share",
        F.lag("seq_no", 1).over(w_running).alias("prev_seq_no"),
        F.lag("sqft_share", 1).over(w_recent).alias("prev_sqft_share"),
        F.avg("sqft_share").over(w5).alias("sqft_share_avg_2"),
        F.cume_dist().over(w6).alias("next_sqft_cume_dist"),
    )
)

matched_keys = (
    with_windows
    .select("region", "id")
    .intersect(
        df_stores
        .filter(F.col("sqft") < 12535)
        .select("region", "id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        "prev_seq_no",
        F.when(F.col("next_sqft_cume_dist") >= 0.63, "HIGH").otherwise("LOW").alias("next_sqft_cume_dist_tier"),
    )
    .agg(
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("region") == "East", 1)), F.lit(0)), 2).alias("seq_no_per_east"),
        F.count("*").alias("event_count"),
        F.percentile_approx("prev_sqft_share", [0.25, 0.5, 0.75]).alias("prev_sqft_share_quartiles"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
    )
    .orderBy(F.col("south_count").desc())
)
