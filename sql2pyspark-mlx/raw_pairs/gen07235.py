from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())
w2 = Window.partitionBy(F.month("effective_from"), "plan").orderBy(F.col("val").asc_nulls_last())
w3 = Window.orderBy(F.col("val").desc_nulls_first())
w4 = Window.partitionBy("plan").orderBy(F.col("greatest_val_cume_dist_val").asc_nulls_last())
w5 = Window.partitionBy("plan")
w6 = Window.partitionBy("plan").orderBy(F.col("val_rank").asc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_limits").alias("val"))
)

metrics = (
    lines
    .filter(F.month("effective_from").isin([2, 9, 10]))
    .select(
        "plan",
        "val",
        F.rank().over(w).alias("val_rank"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
        F.cume_dist().over(w3).alias("val_cume_dist"),
    )
)

filtered = (
    metrics
    .filter(F.col("val_cume_dist").between(0.52, 0.67))
    .select(
        "*",
        F.greatest("val_cume_dist", "val").alias("greatest_val_cume_dist_val"),
        F.coalesce("val", F.lit(0)).alias("val_filled"),
    )
)

metrics_price_tiers = (
    filtered
    .withColumn("greatest_val_cume_dist_val_dense_rank", F.dense_rank().over(w4))
    .withColumn("plan_min_val_filled", F.min("val_filled").over(w5))
    .withColumn("plan_sum_greatest_val_cume_dist_val", F.sum("greatest_val_cume_dist_val").over(w5))
    .withColumn("val_rank_dense_rank", F.dense_rank().over(w6))
)

cleaned = (
    metrics_price_tiers
    .filter(F.col("plan_sum_greatest_val_cume_dist_val") < 197)
    .select(
        "plan",
        "val",
        "val_cume_dist",
        "greatest_val_cume_dist_val",
        "greatest_val_cume_dist_val_dense_rank",
        "plan_min_val_filled",
        F.round((F.col("plan_min_val_filled") * 0.9), 2).alias("adjusted_plan_min_val_filled"),
        F.when(F.col("plan").isin(["enterprise", "pro"]), "Tier 1").otherwise("Other").alias("plan_group"),
    )
)

result = (
    cleaned
    .select(
        "plan",
        F.col("greatest_val_cume_dist_val").alias("GREATEST_VAL_CUME_DIST_VAL"),
        "adjusted_plan_min_val_filled",
        "val_cume_dist",
    )
    .unpivot(
        ids=["plan"],
        values=["GREATEST_VAL_CUME_DIST_VAL", "adjusted_plan_min_val_filled", "val_cume_dist"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
