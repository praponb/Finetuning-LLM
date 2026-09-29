from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("running_min_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("region")

scored = (
    df_stores
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        F.min("sqft").over(w).alias("running_min_sqft"),
        F.sum("sqft").over(w).alias("running_sum_sqft"),
        F.count("sqft").over(w).alias("running_count_sqft"),
        F.max("sqft").over(w).alias("running_max_sqft"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "running_sum_sqft",
        "running_max_sqft",
        F.sum("running_sum_sqft").over(w2).alias("running_sum_running_sum_sqft"),
        F.count("running_sum_sqft").over(w2).alias("running_count_running_sum_sqft"),
        F.min("running_sum_sqft").over(w2).alias("running_min_running_sum_sqft"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.discount").alias("order_discount"),
        F.col("c.order_date").alias("order_order_date"),
    )
)

remaining_keys = (
    with_last
    .select("id")
    .intersect(
        df_stores
        .filter(F.col("city").isin(["London", "Chiang Mai", "Berlin"]))
        .select("id")
    )
)

reconciled = (
    with_last
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "*",
        F.round((F.col("running_count_running_sum_sqft") / F.sum("running_count_running_sum_sqft").over(w4)), 4).alias("running_count_running_sum_sqft_share"),
        F.stddev_samp("running_sum_running_sum_sqft").over(w4).alias("region_stddev_samp_running_sum_running_sum_sqft"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") <= "2024-03-17"), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("running_count_running_sum_sqft_share") >= 0.74, "PREMIUM").otherwise("STANDARD").alias("running_count_running_sum_sqft_share_tier"),
        F.date_trunc("quarter", "opened_date").alias("quarter_start"),
    )
    .agg(
        F.sum(F.when(F.col("region") == "North", F.col("running_sum_running_sum_sqft")).otherwise(0)).alias("north_running_sum_running_sum_sqft"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.percentile_approx("order_discount", 0.9).alias("order_discount_p90"),
        F.percentile_approx("running_sum_running_sum_sqft", 0.5).alias("running_sum_running_sum_sqft_p50"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.percentile_approx("running_sum_running_sum_sqft", [0.25, 0.5, 0.75]).alias("running_sum_running_sum_sqft_quartiles"),
    )
)
