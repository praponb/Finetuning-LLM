from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan")
w_part = Window.orderBy(F.col("val").desc())

exploded = df_price_tiers.select("id", "plan", F.explode_outer("tier_limits").alias("val"))

enriched = (
    exploded
    .select(
        "id",
        "val",
        F.max("val").over(w_ordered).alias("plan_max_val"),
        F.stddev_samp("val").over(w_ordered).alias("plan_stddev_samp_val"),
        F.avg("val").over(w_ordered).alias("plan_avg_val"),
    )
)

enriched_price_tiers = (
    enriched
    .filter(F.col("plan_max_val") == 59)
    .select(
        "val",
        "plan_stddev_samp_val",
        "plan_avg_val",
        F.percent_rank().over(w_part).alias("val_percent_rank"),
    )
)

cleaned = (
    enriched_price_tiers
    .filter(F.col("val") >= 75)
    .select("*", (F.col("val") * F.col("plan_avg_val")).alias("val_times_plan_avg_val"))
)

result = (
    cleaned
    .withColumn("plan_avg_val_tier", F.when(F.col("plan_avg_val") >= 68, "LARGE").otherwise("SMALL"))
    .withColumn("val_times_plan_avg_val_tier", F.when(F.col("val_times_plan_avg_val") >= 76, "HIGH").otherwise("LOW"))
    .withColumn("val_percent_rank_tier", F.when(F.col("val_percent_rank") >= 0.83, "LARGE").otherwise("SMALL"))
    .cube("plan_avg_val_tier", "val_times_plan_avg_val_tier", "val_percent_rank_tier")
    .agg(
        F.percentile_approx("plan_stddev_samp_val", [0.25, 0.5, 0.75]).alias("plan_stddev_samp_val_quartiles"),
        F.percentile_approx("plan_stddev_samp_val", [0.25, 0.5, 0.75]).alias("plan_stddev_samp_val_quartiles_2"),
        F.percentile_approx("plan_stddev_samp_val", 0.95).alias("plan_stddev_samp_val_p95"),
        F.grouping("plan_avg_val_tier").alias("gid"),
    )
    .orderBy(F.col("val_times_plan_avg_val_tier").asc_nulls_last())
)
