from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.orderBy(F.col("qty").desc())
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.orderBy(F.col("qty_cume_dist").desc())
w5 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.orderBy(F.col("qty_cume_dist").asc_nulls_first())
w7 = Window.orderBy(F.col("qty_cume_dist_rank").desc_nulls_last())
w8 = Window.orderBy(F.col("qty_avg_2h_avg_30m").asc_nulls_first())

flattened = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select("id", "created_at", F.col("li_item.qty"))
)

with_windows = (
    flattened
    .withColumn("qty_avg_2h", F.avg("qty").over(w))
    .withColumn("qty_cume_dist", F.cume_dist().over(w2))
)

enriched = (
    with_windows
    .withColumn("qty_avg_2h_avg_30m", F.avg("qty_avg_2h").over(w3))
    .withColumn("qty_cume_dist_rank", F.rank().over(w4))
    .withColumn("qty_avg_2h_max_15m", F.max("qty_avg_2h").over(w5))
    .withColumn("qty_cume_dist_percent_rank", F.percent_rank().over(w6))
)

scored = (
    enriched
    .select(
        "*",
        F.avg("qty_cume_dist_percent_rank").over(w5).alias("qty_cume_dist_percent_rank_avg_15m"),
        F.dense_rank().over(w4).alias("qty_cume_dist_dense_rank"),
        F.rank().over(w7).alias("qty_cume_dist_rank_rank"),
        F.max("qty_cume_dist_rank").over(w).alias("qty_cume_dist_rank_max_2h"),
        F.cume_dist().over(w8).alias("qty_avg_2h_avg_30m_cume_dist"),
    )
)

filtered = (
    scored
    .filter(F.col("created_at") >= F.lit("2025-03-02 15:00:00").cast("timestamp"))
    .withColumn("qty_cume_dist_percent_rank_plus_qty_cume_dist_rank_max_2h", (F.col("qty_cume_dist_percent_rank") + F.col("qty_cume_dist_rank_max_2h")))
)

filtered_carts = (
    filtered
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .select(
        "*",
        F.round((F.col("qty_avg_2h_avg_30m") * 1.15), 2).alias("adjusted_qty_avg_2h_avg_30m"),
        F.round("qty_cume_dist_dense_rank", 2).alias("qty_cume_dist_dense_rank_rounded"),
    )
)

enriched_carts = (
    filtered_carts
    .select(
        "*",
        F.max("qty_cume_dist_rank").over(w3).alias("qty_cume_dist_rank_max_30m"),
        F.count("qty_cume_dist_dense_rank_rounded").over(w).alias("qty_cume_dist_dense_rank_rounded_count_2h"),
    )
)

result = (
    enriched_carts
    .groupBy(
        F.when(F.col("qty_avg_2h_avg_30m") >= 4, "LARGE").otherwise("SMALL").alias("qty_avg_2h_avg_30m_tier"),
    )
    .agg(
        F.min("created_at").alias("first_seen"),
        F.max("created_at").alias("last_created_at"),
        F.max("created_at").alias("ended_at"),
    )
)
