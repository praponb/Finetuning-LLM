from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("quantity_whole", "id")
w_ordered = Window.partitionBy("store_id").orderBy("quantity", "id")
w5 = Window.orderBy(F.col("quantity").asc_nulls_first())
w6 = Window.partitionBy("product_id").orderBy(F.col("next_quantity_per_quantity_whole").asc_nulls_first())
w7 = Window.partitionBy("store_id")
w_grp = Window.partitionBy("product_id").orderBy(F.col("prev_quantity_whole").desc())
w_part = Window.partitionBy(F.date_trunc("WEEK", "last_restock"))
w10 = Window.orderBy("quantity_rank_per_prev_quantity_whole")
w_hist = Window.orderBy(F.col("next_quantity").desc_nulls_last())

base = (
    df_inventory
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-02-05"))) & F.col("quantity").between(185, 188))
    .withColumn("quantity_level", F.when(F.col("quantity") >= 192, "High").when(F.col("quantity") >= 66, "Medium").otherwise("Low"))
    .withColumn("quantity_whole", F.floor("quantity"))
)

metrics = (
    base
    .select(
        "*",
        F.lag("quantity_whole", 1).over(w).alias("prev_quantity_whole"),
        F.sum("quantity").over(w2).alias("cumulative_quantity"),
        F.lead("quantity", 1).over(w3).alias("next_quantity"),
        F.ntile(3).over(w_ordered).alias("quantity_bucket"),
    )
)

filtered = (
    metrics
    .filter(F.col("last_restock") >= F.lit("2023-03-02").cast("date"))
    .withColumn("next_quantity_per_quantity_whole", (F.col("next_quantity") / F.nullif("quantity_whole", F.lit(0))))
    .withColumn("quantity_whole_times_cumulative_quantity", (F.col("quantity_whole") * F.col("cumulative_quantity")))
)

kept_keys = (
    filtered
    .select("store_id", "id")
    .exceptAll(
        df_inventory
        .filter(F.col("store_id").isin([1, 2, 5, 6]))
        .select("store_id", "id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .filter(F.col("store_id").isin([2, 6]))
    .select(
        "id",
        "quantity",
        "prev_quantity_whole",
        "cumulative_quantity",
        "next_quantity",
        "quantity_bucket",
        "next_quantity_per_quantity_whole",
        F.rank().over(w5).alias("quantity_rank"),
        F.rank().over(w6).alias("next_quantity_per_quantity_whole_rank"),
        F.avg("quantity_whole").over(w7).alias("store_id_avg_quantity_whole"),
        F.percent_rank().over(w_grp).alias("prev_quantity_whole_percent_rank"),
        F.min("quantity_whole").over(w_part).alias("last_restock_period_min_quantity_whole"),
    )
)

filtered_inventory = (
    scored
    .filter(F.col("cumulative_quantity") == 1018)
    .select(
        "*",
        (F.col("quantity_rank") / F.nullif("prev_quantity_whole", F.lit(0))).alias("quantity_rank_per_prev_quantity_whole"),
    )
)

metrics_inventory = (
    filtered_inventory
    .select(
        "id",
        "next_quantity",
        "quantity_bucket",
        "next_quantity_per_quantity_whole",
        "prev_quantity_whole_percent_rank",
        "last_restock_period_min_quantity_whole",
        "quantity_rank_per_prev_quantity_whole",
        F.cume_dist().over(w10).alias("quantity_rank_per_prev_quantity_whole_cume_dist"),
        F.rank().over(w5).alias("quantity_rank_2"),
        F.rank().over(w_hist).alias("next_quantity_rank"),
    )
)

daily = (
    metrics_inventory
    .rollup(
        F.when(F.col("next_quantity") >= 221, "HIGH").otherwise("LOW").alias("next_quantity_tier"),
    )
    .agg(
        F.percentile_approx("quantity_rank_per_prev_quantity_whole_cume_dist", [0.25, 0.5, 0.75]).alias("quantity_rank_per_prev_quantity_whole_cume_dist_quartiles"),
        F.count("*").alias("row_count"),
        F.percentile_approx("next_quantity", [0.25, 0.5, 0.75]).alias("next_quantity_quartiles"),
        F.percentile_approx("prev_quantity_whole_percent_rank", 0.5).alias("prev_quantity_whole_percent_rank_p50"),
        F.percentile_approx("quantity_rank_per_prev_quantity_whole_cume_dist", [0.25, 0.5, 0.75]).alias("quantity_rank_per_prev_quantity_whole_cume_dist_quartiles_2"),
        F.max("quantity_rank_per_prev_quantity_whole_cume_dist").alias("max_quantity_rank_per_prev_quantity_whole_cume_dist"),
    )
)

result = (
    daily
    .select(
        "next_quantity_tier",
        "quantity_rank_per_prev_quantity_whole_cume_dist_quartiles",
        "row_count",
        "next_quantity_quartiles",
        "prev_quantity_whole_percent_rank_p50",
        "quantity_rank_per_prev_quantity_whole_cume_dist_quartiles_2",
        "max_quantity_rank_per_prev_quantity_whole_cume_dist",
    )
)
