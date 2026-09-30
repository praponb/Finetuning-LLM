from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy(F.col("val").desc())
w2 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy(F.month("effective_from"), "plan")
w5 = Window.partitionBy(F.month("effective_from"), "plan").orderBy(F.col("val").desc())
w6 = Window.partitionBy("plan").orderBy(F.col("effective_from_month_max_val").desc_nulls_first())
w7 = Window.orderBy(F.col("val_dense_rank_2").asc_nulls_last())
w8 = Window.partitionBy("plan").orderBy(F.col("effective_from_month_max_val_percent_rank").asc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_limits").alias("val"))
)

with_windows = (
    exploded
    .select(
        "id",
        "plan",
        F.dense_rank().over(w_ordered).alias("val_dense_rank"),
        F.dense_rank().over(w2).alias("val_dense_rank_2"),
        F.avg("val").over(w3).alias("plan_avg_val"),
        F.max("val").over(w4).alias("effective_from_month_max_val"),
        F.percent_rank().over(w5).alias("val_percent_rank"),
    )
)

enriched = (
    with_windows
    .withColumn("effective_from_month_max_val_percent_rank", F.percent_rank().over(w6))
    .withColumn("val_dense_rank_2_dense_rank", F.dense_rank().over(w7))
)

metrics = (
    enriched
    .withColumn("plan_count_val_percent_rank", F.count("val_percent_rank").over(w3))
    .withColumn("plan_avg_effective_from_month_max_val", F.avg("effective_from_month_max_val").over(w3))
    .withColumn("effective_from_month_max_val_percent_rank_cume_dist", F.cume_dist().over(w8))
)

kept_keys = (
    metrics
    .select("id", "plan")
    .exceptAll(
        df_price_tiers
        .filter(F.col("effective_from") < "2023-08-01")
        .select("id", "plan")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    verified
    .rollup("plan")
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.round(F.avg("val_dense_rank_2_dense_rank"), 2).alias("avg_val_dense_rank_2_dense_rank"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count("*").alias("cnt"),
        F.round(F.sum("plan_count_val_percent_rank") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("plan_count_val_percent_rank_per_pro"),
    )
    .orderBy(F.col("avg_val_dense_rank_2_dense_rank").desc())
)
