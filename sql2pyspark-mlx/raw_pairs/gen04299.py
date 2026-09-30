from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("city").orderBy("sqft", "id")
w_running_2 = Window.partitionBy(F.date_trunc("month", "inventory_last_restock"), "city").orderBy("sqft", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("region").orderBy("inventory_last_restock", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("region").orderBy("sqft_avg_6_prior", "id").rowsBetween(-6, Window.currentRow)
w_ordered = Window.partitionBy(F.date_trunc("YEAR", "inventory_last_restock"), "city")
w7 = Window.partitionBy(F.year("inventory_last_restock"), "city").orderBy("sqft_avg_3", "id").rowsBetween(-6, -1)
w8 = Window.partitionBy("region").orderBy("inventory_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy(F.date_trunc("YEAR", "inventory_last_restock"))
w10 = Window.partitionBy("region", "city").orderBy("sqft_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("region").orderBy(F.col("last7_avg_sqft").asc_nulls_last())

filtered = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.lower("region").alias("region_lower"),
        F.col("sqft").cast("string").alias("sqft_str"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock").alias("inventory_last_restock"))
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.customer_id").isNull()), "left_anti")
)

with_windows = (
    not_flagged
    .withColumn("next_sqft", F.lead("sqft", 1).over(w_running))
    .withColumn("sqft_avg_6_prior", F.avg("sqft").over(w_running_2))
    .withColumn("sqft_avg_3", F.avg("sqft").over(w4))
)

scored = (
    with_windows
    .filter(F.col("city") != "Chiang Mai")
    .withColumn("last7_max_sqft", F.max("sqft").over(w_grp))
    .withColumn("last7_sum_sqft", F.sum("sqft").over(w_grp))
    .withColumn("last7_avg_sqft", F.avg("sqft").over(w_grp))
    .withColumn("inventory_last_restock_period_min_sqft_avg_6_prior", F.min("sqft_avg_6_prior").over(w_ordered))
)

metrics = (
    scored
    .select(
        "*",
        F.avg("sqft_avg_6_prior").over(w7).alias("sqft_avg_6_prior_avg_6_prior"),
        F.last("sqft", ignorenulls=True).over(w8).alias("last_known_sqft"),
        F.max("last7_avg_sqft").over(w9).alias("inventory_last_restock_period_max_last7_avg_sqft"),
        F.sum("last7_sum_sqft").over(w10).alias("cumulative_last7_sum_sqft"),
        F.rank().over(w11).alias("last7_avg_sqft_rank"),
    )
)

kept_keys = (
    metrics
    .select("id", "city")
    .subtract(
        df_stores
        .filter(F.col("city").isin(["Bangkok", "Berlin", "Singapore"]))
        .select("id", "city")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

stats = (
    verified
    .groupBy(
        F.when(F.col("sqft") >= 17393, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
        F.when(F.col("last7_avg_sqft_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("last7_avg_sqft_rank_tier"),
        F.date_trunc("MONTH", "inventory_last_restock").alias("activity_month"),
    )
    .agg(
        F.size(F.collect_set("region")).alias("num_regions"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
    )
)

result = (
    stats
    .select(
        "sqft_tier",
        "last7_avg_sqft_rank_tier",
        "activity_month",
        "num_regions",
        "bangkok_rows",
    )
    .orderBy(F.col("sqft_tier").desc(), F.col("last7_avg_sqft_rank_tier").desc())
)
