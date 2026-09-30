from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc())
w_ordered = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("element"),
    )
)

metrics = flattened.select("id", "plan", F.dense_rank().over(w).alias("element_dense_rank"))

cleaned = (
    metrics
    .filter(F.col("plan").isin(["enterprise", "pro", "starter"]))
    .withColumn("element_dense_rank_rounded", F.round("element_dense_rank", 2))
)

valid = (
    cleaned
    .filter(F.col("element_dense_rank_rounded").between(29, 40))
    .select("*", F.lower("plan").alias("plan_lower"))
)

enriched = (
    valid
    .select(
        "*",
        F.sum("element_dense_rank").over(w_ordered).alias("plan_sum_element_dense_rank"),
        F.avg("element_dense_rank_rounded").over(w_ordered).alias("plan_avg_element_dense_rank_rounded"),
        F.avg("element_dense_rank_rounded").over(w_ordered).alias("plan_avg_element_dense_rank_rounded_2"),
        F.stddev_samp("element_dense_rank").over(w_ordered).alias("plan_stddev_samp_element_dense_rank"),
    )
)

summary = (
    enriched
    .groupBy("plan")
    .agg(
        F.round(F.sum("plan_avg_element_dense_rank_rounded_2") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("plan_avg_element_dense_rank_rounded_2_per_enterprise"),
        F.max(F.when(F.col("plan") == "starter", F.col("element_dense_rank_rounded"))).alias("starter_max_element_dense_rank_rounded"),
        F.avg(F.when(F.col("plan") == "basic", F.col("element_dense_rank"))).alias("basic_avg_element_dense_rank"),
        F.round(F.sum("plan_avg_element_dense_rank_rounded") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("plan_avg_element_dense_rank_rounded_per_basic"),
    )
    .filter(F.col("plan_avg_element_dense_rank_rounded_per_basic") >= 0)
)

result = (
    summary
    .select(
        "plan",
        "starter_max_element_dense_rank_rounded",
        "basic_avg_element_dense_rank",
        "plan_avg_element_dense_rank_rounded_per_basic",
        F.when(F.col("plan").rlike("^(basic|enterprise)$"), "CORE").otherwise("PRIORITY").alias("plan_zone"),
    )
    .orderBy(F.col("plan_zone").asc_nulls_last())
)
