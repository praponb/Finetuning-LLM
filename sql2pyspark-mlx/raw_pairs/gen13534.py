from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "opened_date"), "city").orderBy(F.col("sqft").desc_nulls_first())
w2 = Window.partitionBy("city").orderBy("sqft", "id")
w3 = Window.partitionBy("region").orderBy("opened_date", "id")
w_hist = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.partitionBy("region")
w6 = Window.partitionBy("last3_min_sqft_tier").orderBy(F.col("max_sqft_percent_rank").desc())

metrics = (
    df_stores
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.percent_rank().over(w).alias("sqft_percent_rank"),
        F.lag("sqft", 2).over(w2).alias("prev_sqft"),
        F.lag("sqft", 2).over(w3).alias("prev_sqft_2"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "left_semi")
)

not_flagged_stores = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "left_semi")
)

cleaned = (
    not_flagged_stores
    .filter(F.col("region").isNotNull() & F.col("store_name").like("%Tow%"))
    .withColumn("opened_date_label", F.date_format("opened_date", "yyyy-MM-dd"))
    .withColumn("region_clean", F.coalesce("region", F.lit("UNKNOWN")))
)

enriched = (
    cleaned
    .select(
        "id",
        "store_name",
        "sqft",
        "sqft_percent_rank",
        "prev_sqft_2",
        "region_clean",
        F.avg("sqft").over(w_hist).alias("last3_avg_sqft"),
        F.count("sqft").over(w_hist).alias("last3_count_sqft"),
        F.min("sqft").over(w_hist).alias("last3_min_sqft"),
        F.avg("sqft_percent_rank").over(w_ordered).alias("region_avg_sqft_percent_rank"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("last3_min_sqft") >= 16500, "PREMIUM").otherwise("STANDARD").alias("last3_min_sqft_tier"),
        F.when(F.col("prev_sqft_2") >= 12899, "LARGE").otherwise("SMALL").alias("prev_sqft_2_tier"),
        F.when(F.col("sqft") >= 15757, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.max("sqft_percent_rank").alias("max_sqft_percent_rank"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .withColumn("max_sqft_percent_rank_dense_rank", F.dense_rank().over(w6))
    .orderBy(F.col("distinct_ids_2").desc_nulls_first())
)
