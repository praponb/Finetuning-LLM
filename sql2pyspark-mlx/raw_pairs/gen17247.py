from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy(F.month("opened_date"), "region").orderBy("sqft", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("WEEK", "opened_date"), "region")
w3 = Window.orderBy(F.col("sqft_avg_3_plus_sqft").desc_nulls_last())
w4 = Window.partitionBy(F.date_trunc("month", "opened_date")).orderBy(F.col("sqft_share").desc_nulls_first())
w5 = Window.orderBy(F.col("sqft_avg_3_plus_sqft").desc_nulls_first())
w6 = Window.orderBy(F.col("sqft_avg_3_plus_sqft").asc_nulls_first())
w7 = Window.orderBy(F.col("sqft_avg_3_plus_sqft").desc())

metrics = (
    df_stores
    .withColumn("sqft_avg_3", F.avg("sqft").over(w_grp))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4))
)

remaining_keys = (
    metrics
    .select("id")
    .subtract(
        df_stores
        .filter(F.col("sqft").between(7623, 7975))
        .select("id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") <= F.col("s.opened_date")), "left_semi")
)

cleaned = (
    eligible
    .filter((F.col("opened_date") >= F.lit("2023-04-28").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        "sqft_share",
        (F.col("sqft_avg_3") + F.col("sqft")).alias("sqft_avg_3_plus_sqft"),
        F.coalesce("region", F.lit("Unknown")).alias("region_filled"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w3).alias("sqft_avg_3_plus_sqft_dense_rank"),
        F.cume_dist().over(w4).alias("sqft_share_cume_dist"),
        F.dense_rank().over(w5).alias("sqft_avg_3_plus_sqft_dense_rank_2"),
        F.dense_rank().over(w6).alias("sqft_avg_3_plus_sqft_dense_rank_3"),
        F.cume_dist().over(w7).alias("sqft_avg_3_plus_sqft_cume_dist"),
    )
)

base = (
    enriched
    .filter(F.col("sqft_share_cume_dist") >= 0.69)
    .select(
        "id",
        "store_name",
        "sqft",
        "sqft_share",
        "sqft_avg_3_plus_sqft",
        "region_filled",
        "sqft_avg_3_plus_sqft_dense_rank",
        "sqft_avg_3_plus_sqft_dense_rank_3",
        "sqft_avg_3_plus_sqft_cume_dist",
        F.add_months("opened_date", 3).alias("opened_date_plus_months"),
        (F.col("sqft_share") / F.nullif("sqft_avg_3_plus_sqft_cume_dist", F.lit(0))).alias("sqft_share_per_sqft_avg_3_plus_sqft_cume_dist"),
    )
)

result = (
    base
    .withColumn("sqft_share_tier", F.when(F.col("sqft_share") >= 0.61, "LARGE").otherwise("SMALL"))
    .withColumn("year_start", F.date_trunc("YEAR", "opened_date_plus_months"))
    .groupingSets(
        [["sqft_share_tier", "year_start"], ["sqft_share_tier"]],
        "sqft_share_tier",
        "year_start",
    )
    .agg(
        F.avg("sqft_avg_3_plus_sqft_dense_rank_3").alias("avg_sqft_avg_3_plus_sqft_dense_rank_3"),
        F.percentile_approx("sqft_share", [0.25, 0.5, 0.75]).alias("sqft_share_quartiles"),
        F.avg("sqft_avg_3_plus_sqft_dense_rank_3").alias("avg_sqft_avg_3_plus_sqft_dense_rank_3_2"),
        F.grouping("sqft_share_tier").alias("gid"),
    )
)
