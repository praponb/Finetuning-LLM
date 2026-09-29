from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc_nulls_first(), "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").asc_nulls_last(), "id")
w5 = Window.partitionBy("product_id").orderBy("quantity", "id")
w6 = Window.partitionBy("store_id").orderBy("row_seq", "id").rowsBetween(-2, Window.currentRow)

deduped = (
    df_inventory
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.price"))
)

metrics = (
    enriched
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("last_known_price", F.last("price", ignorenulls=True).over(w3))
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_windows = (
    latest
    .select(
        "*",
        F.row_number().over(w5).alias("event_seq"),
        F.avg("last_known_price").over(w6).alias("last_known_price_avg_2"),
    )
)

matched_keys = (
    with_windows
    .select("store_id", "id")
    .exceptAll(
        df_inventory
        .filter(F.col("last_restock") < "2025-09-01")
        .select("store_id", "id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .rollup("store_id")
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.grouping("store_id").alias("grouping_level"),
    )
)
