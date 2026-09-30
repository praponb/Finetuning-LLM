from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())
w2 = Window.orderBy(F.col("element").desc_nulls_first())
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("plan").orderBy(F.col("element_dense_rank_2").asc_nulls_first())
w5 = Window.orderBy(F.col("plan_max_element_dense_rank_2").desc_nulls_first())

exploded = df_price_tiers.select("id", "plan", F.explode_outer("tier_limits").alias("element"))

enriched = (
    exploded
    .select(
        "*",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.dense_rank().over(w2).alias("element_dense_rank_2"),
    )
)

scored = (
    enriched
    .filter(F.col("plan") != "pro")
    .withColumn("plan_max_element_dense_rank_2", F.max("element_dense_rank_2").over(w3))
    .withColumn("plan_max_element_cume_dist", F.max("element_cume_dist").over(w3))
    .withColumn("element_dense_rank_2_cume_dist", F.cume_dist().over(w4))
)

scored_price_tiers = scored.withColumn("plan_max_element_dense_rank_2_rank", F.rank().over(w5))

cleaned = (
    scored_price_tiers
    .filter(F.col("plan").isin(["enterprise", "starter", "pro"]))
    .select(
        "plan",
        "element",
        "element_dense_rank",
        "element_cume_dist",
        "element_dense_rank_2",
        "plan_max_element_cume_dist",
        "element_dense_rank_2_cume_dist",
        "plan_max_element_dense_rank_2_rank",
        (F.col("plan_max_element_dense_rank_2_rank") * F.col("element")).alias("plan_max_element_dense_rank_2_rank_times_element"),
    )
)

result = (
    cleaned
    .select(
        "plan",
        "element",
        "element_dense_rank",
        "element_cume_dist",
        "plan_max_element_cume_dist",
        "element_dense_rank_2_cume_dist",
        "plan_max_element_dense_rank_2_rank",
        "plan_max_element_dense_rank_2_rank_times_element",
        F.when(F.col("plan").rlike("^(enterprise|basic)$"), "EXTENDED").when(F.col("plan").rlike("^(starter|pro)$"), "CORE").otherwise("OTHER_ZONE").alias("plan_zone"),
    )
)
