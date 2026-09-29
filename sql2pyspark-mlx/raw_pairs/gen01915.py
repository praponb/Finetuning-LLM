from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("currency", "plan")
w3 = Window.partitionBy("plan").orderBy(F.col("plan_sum_val").desc_nulls_last())
w_ordered = Window.partitionBy("currency").orderBy(F.col("plan_sum_val").desc())
w5 = Window.partitionBy("plan").orderBy(F.col("pro_events").desc())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

with_windows = exploded.select("*", F.sum("val").over(w).alias("plan_sum_val"))

metrics = (
    with_windows
    .select(
        "id",
        "plan",
        "currency",
        F.max("val").over(w2).alias("currency_max_val"),
        F.cume_dist().over(w3).alias("plan_sum_val_cume_dist"),
        F.rank().over(w_ordered).alias("plan_sum_val_rank"),
    )
)

cleaned = (
    metrics
    .filter(F.col("currency").isin(["THB", "USD", "EUR"]))
    .select(
        "id",
        "plan",
        "currency_max_val",
        "plan_sum_val_rank",
        F.concat_ws(",", "plan", "currency").alias("plan_currency_key"),
        F.round("plan_sum_val_rank", 2).alias("plan_sum_val_rank_rounded"),
    )
)

grouped = (
    cleaned
    .groupBy(
        "plan",
        F.when(F.col("plan_sum_val_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("plan_sum_val_rank_tier"),
    )
    .agg(
        F.round(F.avg("plan_sum_val_rank_rounded"), 2).alias("avg_plan_sum_val_rank_rounded"),
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
    .withColumn("pro_events_dense_rank", F.dense_rank().over(w5))
)

result = grouped.select("plan", "plan_sum_val_rank_tier", "pro_events_dense_rank")
