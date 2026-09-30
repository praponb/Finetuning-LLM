from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("quantity")
w2 = Window.partitionBy("store_id").orderBy("latest_rank", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy("last_restock", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("product_id").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("latest_rank", "id")
w7 = Window.orderBy("latest_rank_rounded_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.year("last_restock"), "product_id").orderBy("latest_rank_rounded_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("store_id", "product_id").orderBy("latest_rank")
w10 = Window.partitionBy("product_id").orderBy(F.col("latest_rank_rounded_avg_6").desc(), "id")
w11 = Window.partitionBy(F.date_trunc("YEAR", "last_restock"), "store_id")
w12 = Window.partitionBy("product_id").orderBy("latest_rank_rounded_avg_6_prior", "id").rowsBetween(-2, -1)
w13 = Window.partitionBy("store_id").orderBy("latest_rank_rounded_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-07-23"))) & F.col("quantity").between(118, 129))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 198, "High").when(F.col("quantity") >= 38, "Medium").otherwise("Low"))
)

latest = (
    filtered
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

valid = (
    latest
    .filter(F.col("latest_rank") < 1)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_tier",
        "latest_rank",
        F.round("latest_rank", 1).alias("latest_rank_rounded"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.avg("latest_rank_rounded").over(w2).alias("latest_rank_rounded_avg_6"),
        F.avg("latest_rank_rounded").over(w3).alias("latest_rank_rounded_avg_6_prior"),
        F.first("latest_rank_rounded", ignorenulls=True).over(w4).alias("first_known_latest_rank_rounded"),
        F.sum("latest_rank_rounded").over(w5).alias("cumulative_latest_rank_rounded"),
        F.lag("quantity", 1).over(w6).alias("prev_quantity"),
    )
)

metrics_inventory = (
    metrics
    .filter(F.col("last_restock") > "2025-10-26")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "latest_rank",
        "latest_rank_rounded",
        "latest_rank_rounded_avg_6",
        "latest_rank_rounded_avg_6_prior",
        "first_known_latest_rank_rounded",
        "cumulative_latest_rank_rounded",
        F.sum("latest_rank_rounded_avg_6").over(w7).alias("cumulative_latest_rank_rounded_avg_6"),
        F.sum("first_known_latest_rank_rounded").over(w8).alias("running_first_known_latest_rank_rounded"),
    )
)

enriched = (
    metrics_inventory
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.sqft"))
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w9))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "store_id",
        "latest_rank",
        "latest_rank_rounded_avg_6_prior",
        "first_known_latest_rank_rounded",
        "running_first_known_latest_rank_rounded",
        "sqft",
        "dedup_rank",
        F.row_number().over(w10).alias("event_seq"),
        F.min("first_known_latest_rank_rounded").over(w11).alias("last_restock_period_min_first_known_latest_rank_rounded"),
        F.avg("running_first_known_latest_rank_rounded").over(w12).alias("running_first_known_latest_rank_rounded_avg_2_prior"),
        F.last("quantity", ignorenulls=True).over(w13).alias("last_known_quantity"),
    )
)
