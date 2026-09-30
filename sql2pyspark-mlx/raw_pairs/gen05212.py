from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").asc_nulls_last(), "id")
w2 = Window.orderBy("last_restock", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("store_id").orderBy("adjusted_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.month("last_restock"), "store_id").orderBy(F.col("adjusted_quantity").desc_nulls_first())
w5 = Window.partitionBy("store_id")
w6 = Window.partitionBy("store_id").orderBy(F.col("running_adjusted_quantity").desc_nulls_last())
w7 = Window.partitionBy(F.year("last_restock")).orderBy(F.col("adjusted_quantity_avg_6_prior").desc_nulls_first())
w8 = Window.partitionBy("store_id").orderBy(F.col("adjusted_quantity_avg_6_prior").desc_nulls_first())

valid = (
    df_inventory
    .filter(F.col("quantity") > 75)
    .select(
        "id",
        "store_id",
        "last_restock",
        F.round((F.col("quantity") * 1.1), 2).alias("adjusted_quantity"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    deduped
    .withColumn("adjusted_quantity_avg_6_prior", F.avg("adjusted_quantity").over(w2))
    .withColumn("running_adjusted_quantity", F.sum("adjusted_quantity").over(w3))
)

matched_keys = (
    enriched
    .select("store_id", "id")
    .intersect(
        df_inventory
        .filter(F.col("quantity") >= 136)
        .select("store_id", "id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "id",
        "store_id",
        "last_restock",
        "adjusted_quantity",
        "running_adjusted_quantity",
        F.cume_dist().over(w4).alias("adjusted_quantity_cume_dist"),
        F.min("running_adjusted_quantity").over(w5).alias("store_id_min_running_adjusted_quantity"),
        F.percent_rank().over(w6).alias("running_adjusted_quantity_percent_rank"),
        F.percent_rank().over(w7).alias("adjusted_quantity_avg_6_prior_percent_rank"),
        F.dense_rank().over(w8).alias("adjusted_quantity_avg_6_prior_dense_rank"),
    )
)

result = (
    metrics
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("activity_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("adjusted_quantity_cume_dist", 0.95).alias("adjusted_quantity_cume_dist_p95"),
        F.sum("adjusted_quantity").alias("total_adjusted_quantity"),
        F.percentile_approx("adjusted_quantity_avg_6_prior_dense_rank", 0.5).alias("adjusted_quantity_avg_6_prior_dense_rank_p50"),
        F.round(F.avg("adjusted_quantity_cume_dist"), 2).alias("avg_adjusted_quantity_cume_dist"),
    )
)
