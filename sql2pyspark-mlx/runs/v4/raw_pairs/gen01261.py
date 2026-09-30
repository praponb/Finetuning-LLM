from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.partitionBy("currency")
w3 = Window.orderBy("tier_value")
w4 = Window.partitionBy("plan")
w5 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("tier_value"))
)

with_windows = (
    flattened
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w))
    .withColumn("currency_min_tier_value", F.min("tier_value").over(w2))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w3))
    .withColumn("plan_sum_tier_value", F.sum("tier_value").over(w4))
    .withColumn("tier_value_rank", F.rank().over(w5))
)

base = (
    with_windows
    .filter(F.col("currency_min_tier_value") < 30)
    .select(
        "*",
        (F.col("plan_sum_tier_value") + F.col("tier_value")).alias("plan_sum_tier_value_plus_tier_value"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("plan_sum_tier_value") >= 402, "LARGE").otherwise("SMALL").alias("plan_sum_tier_value_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_count"),
        F.count_if(F.col("currency") == "THB").alias("thb_events"),
        F.round(F.avg("currency_min_tier_value"), 2).alias("avg_currency_min_tier_value"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
    )
    .orderBy(F.col("plan_sum_tier_value_tier").desc())
)
