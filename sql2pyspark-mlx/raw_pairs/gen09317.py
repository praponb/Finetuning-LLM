from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "region")

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-03-27").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "store_id",
        "order_date",
        "amount",
        "discount",
        F.when(F.col("shipping_fee") >= 41, "High").when(F.col("shipping_fee") >= 11, "Medium").otherwise("Low").alias("shipping_fee_tier"),
    )
)

filtered_orders = (
    filtered
    .filter(F.col("amount").isNull())
    .withColumn("discount_minus_amount", (F.col("discount") - F.col("amount")))
)

joined = (
    filtered_orders
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .filter(F.col("b.sqft") == 6943)
    .select("e.*", F.col("b.sqft"), F.col("b.region"), F.col("b.opened_date"))
)

with_windows = (
    joined
    .select(
        "*",
        F.max("discount_minus_amount").over(w).alias("running_max_discount_minus_amount"),
        F.count("discount_minus_amount").over(w).alias("running_count_discount_minus_amount"),
        F.avg("discount_minus_amount").over(w).alias("running_avg_discount_minus_amount"),
        F.min("discount_minus_amount").over(w).alias("running_min_discount_minus_amount"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
    )
)

matched_keys = (
    with_windows
    .select("store_id", "id")
    .intersectAll(
        df_orders
        .filter(F.col("store_id") == 6)
        .select("store_id", "id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

stats = (
    kept_rows
    .withColumn("year_start", F.date_trunc("YEAR", "opened_date"))
    .cube("store_id", "year_start")
    .agg(
        F.percentile_approx("sqft_share", [0.25, 0.5, 0.75]).alias("sqft_share_quartiles"),
        F.percentile_approx("sqft_share", [0.25, 0.5, 0.75]).alias("sqft_share_quartiles_2"),
        F.count_if(F.col("region") == "South").alias("south_events"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.percentile_approx("sqft_share", [0.25, 0.5, 0.75]).alias("sqft_share_quartiles_3"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("store_id").alias("gid"),
    )
)

result = (
    stats
    .select(
        "store_id",
        "year_start",
        "sqft_share_quartiles_2",
        "south_events",
        "sqft_share_quartiles_3",
        "distinct_ids",
    )
    .orderBy(F.col("south_events").desc())
)
