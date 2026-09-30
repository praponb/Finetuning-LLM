from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("val").desc())
w3 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w_ordered = Window.partitionBy("plan").orderBy(F.col("plan_sum_val").desc_nulls_last())
w5 = Window.orderBy(F.col("plan_sum_val").asc_nulls_last())
w_running = Window.partitionBy("plan").orderBy(F.col("plan_count_val").desc_nulls_first())
w7 = Window.partitionBy("plan").orderBy(F.col("plan_sum_val").desc_nulls_first())
w8 = Window.partitionBy("plan").orderBy(F.col("plan_min_is_basic").asc_nulls_last())

exploded = df_price_tiers.select("id", "plan", F.explode_outer("tier_limits").alias("val"))

enriched = (
    exploded
    .withColumn("plan_max_val", F.max("val").over(w_hist))
    .withColumn("plan_sum_val", F.sum("val").over(w_hist))
    .withColumn("val_rank", F.rank().over(w2))
    .withColumn("plan_count_val", F.count("val").over(w_hist))
    .withColumn("val_rank_2", F.rank().over(w3))
)

filtered = (
    enriched
    .filter(F.col("plan") == "basic")
    .select(
        "plan",
        "plan_sum_val",
        "val_rank",
        "plan_count_val",
        F.when(F.col("val") >= 89, "High").when(F.col("val") >= 24, "Medium").otherwise("Low").alias("val_band"),
    )
)

base = (
    filtered
    .filter(F.col("plan") == "basic")
    .select(
        "plan",
        "plan_sum_val",
        "plan_count_val",
        F.when(F.col("plan") == "basic", 1).otherwise(0).alias("is_basic"),
        F.upper("plan").alias("plan_upper"),
    )
)

scored = (
    base
    .select(
        "*",
        F.rank().over(w_ordered).alias("plan_sum_val_rank"),
        F.dense_rank().over(w5).alias("plan_sum_val_dense_rank"),
        F.min("is_basic").over(w_hist).alias("plan_min_is_basic"),
        F.rank().over(w_running).alias("plan_count_val_rank"),
    )
)

metrics = (
    scored
    .select(
        "plan",
        "plan_sum_val",
        "plan_count_val",
        "plan_upper",
        "plan_sum_val_rank",
        "plan_sum_val_dense_rank",
        "plan_count_val_rank",
        F.sum("is_basic").over(w_hist).alias("plan_sum_is_basic"),
        F.cume_dist().over(w7).alias("plan_sum_val_cume_dist"),
        F.dense_rank().over(w8).alias("plan_min_is_basic_dense_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("plan_count_val") >= 16, "PREMIUM").otherwise("STANDARD").alias("plan_count_val_tier"),
        "plan",
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("plan_count_val", 0.95).alias("plan_count_val_p95"),
        F.count("*").alias("event_count"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
)
