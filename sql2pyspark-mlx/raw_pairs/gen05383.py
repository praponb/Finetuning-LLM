from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc())
w_part = Window.orderBy(F.col("cost_dense_rank").asc_nulls_first())
w_hist = Window.orderBy(F.col("cost_dense_rank").desc_nulls_first())
w_ordered = Window.orderBy(F.col("cost_dense_rank_rank").asc_nulls_last())
w5 = Window.orderBy(F.col("cost_dense_rank_rank").asc_nulls_first())
w6 = Window.orderBy(F.col("cost_dense_rank_cume_dist").desc())
w7 = Window.orderBy(F.col("cost_dense_rank_rank_percent_rank").asc_nulls_last())
w8 = Window.orderBy(F.col("cost_dense_rank_rank_percent_rank_2").asc_nulls_first())
w_hist_2 = Window.orderBy(F.col("cost_dense_rank_rank_percent_rank").desc_nulls_last())

cleaned = (
    df_products
    .filter(F.col("stock_qty") <= 111)
    .select(
        "id",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.array_join("tags", "|").alias("tags_text"),
        (F.col("stock_qty") * F.col("price")).alias("stock_qty_times_price"),
    )
)

matched_keys = (
    cleaned
    .select("supplier_id", "id")
    .subtract(
        df_products
        .filter(F.col("price").between(390, 980))
        .select("supplier_id", "id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = reconciled.select("id", "tags", F.dense_rank().over(w).alias("cost_dense_rank"))

scored_products = scored.withColumn("cost_dense_rank_rank", F.rank().over(w_part))

metrics = (
    scored_products
    .filter(F.col("cost_dense_rank_rank").isNotNull())
    .select(
        "id",
        "cost_dense_rank_rank",
        F.cume_dist().over(w_hist).alias("cost_dense_rank_cume_dist"),
        F.percent_rank().over(w_ordered).alias("cost_dense_rank_rank_percent_rank"),
        F.rank().over(w_hist).alias("cost_dense_rank_rank_2"),
        F.percent_rank().over(w_ordered).alias("cost_dense_rank_rank_percent_rank_2"),
    )
)

with_windows = (
    metrics
    .select(
        "cost_dense_rank_cume_dist",
        "cost_dense_rank_rank_percent_rank",
        "cost_dense_rank_rank_2",
        "cost_dense_rank_rank_percent_rank_2",
        F.percent_rank().over(w5).alias("cost_dense_rank_rank_percent_rank_3"),
        F.dense_rank().over(w6).alias("cost_dense_rank_cume_dist_dense_rank"),
        F.percent_rank().over(w7).alias("cost_dense_rank_rank_percent_rank_percent_rank"),
        F.rank().over(w8).alias("cost_dense_rank_rank_percent_rank_2_rank"),
        F.percent_rank().over(w_hist_2).alias("cost_dense_rank_rank_percent_rank_percent_rank_2"),
    )
)

stats = (
    with_windows
    .groupBy(
        F.when(F.col("cost_dense_rank_cume_dist") >= 0.67, "HIGH").otherwise("LOW").alias("cost_dense_rank_cume_dist_tier"),
        F.when(F.col("cost_dense_rank_rank_percent_rank") >= 0.87, "PREMIUM").otherwise("STANDARD").alias("cost_dense_rank_rank_percent_rank_tier"),
    )
    .agg(
        F.round(F.avg("cost_dense_rank_rank_2"), 2).alias("avg_cost_dense_rank_rank_2"),
        F.count("*").alias("cnt"),
        F.percentile_approx("cost_dense_rank_rank_percent_rank_2_rank", 0.9).alias("cost_dense_rank_rank_percent_rank_2_rank_p90"),
        F.percentile_approx("cost_dense_rank_rank_percent_rank_2", [0.25, 0.5, 0.75]).alias("cost_dense_rank_rank_percent_rank_2_quartiles"),
    )
)

result = (
    stats
    .select(
        "cost_dense_rank_cume_dist_tier",
        "cost_dense_rank_rank_percent_rank_tier",
        "cnt",
    )
    .orderBy("cost_dense_rank_cume_dist_tier")
)
