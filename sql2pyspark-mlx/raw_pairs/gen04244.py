from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("element_cume_dist_filled").asc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("element").desc())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("element"))
)

enriched = (
    exploded
    .filter(F.col("currency").isin(["EUR", "THB"]))
    .withColumn("element_cume_dist", F.cume_dist().over(w))
)

base = (
    enriched
    .filter(F.col("element_cume_dist").between(0.4, 0.59))
    .select(
        "*",
        F.coalesce("element_cume_dist", F.lit(0)).alias("element_cume_dist_filled"),
    )
)

metrics = base.select("*", F.rank().over(w2).alias("element_cume_dist_filled_rank"))

enriched_price_tiers = (
    metrics
    .select(
        "plan",
        "element_cume_dist_filled_rank",
        F.cume_dist().over(w3).alias("element_cume_dist_2"),
    )
)

filtered = (
    enriched_price_tiers
    .filter(F.col("element_cume_dist_2") >= 0.21)
    .withColumn("element_cume_dist_2_minus_element_cume_dist_filled_rank", (F.col("element_cume_dist_2") - F.col("element_cume_dist_filled_rank")))
    .withColumn("element_cume_dist_filled_rank_whole", F.floor("element_cume_dist_filled_rank"))
)

result = (
    filtered
    .groupBy("plan")
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.sum("element_cume_dist_filled_rank_whole").alias("total_element_cume_dist_filled_rank_whole"),
        F.round(F.avg("element_cume_dist_2_minus_element_cume_dist_filled_rank"), 2).alias("avg_element_cume_dist_2_minus_element_cume_dist_filled_rank"),
    )
    .filter(F.col("starter_rows") < 1000)
    .orderBy(F.col("plan").desc_nulls_first())
)
