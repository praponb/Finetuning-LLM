from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_first())
w_hist = Window.orderBy(F.col("element_cume_dist").desc_nulls_first())
w3 = Window.orderBy(F.col("element_cume_dist").desc())
w4 = Window.orderBy(F.col("element_cume_dist_rank").desc_nulls_last())
w5 = Window.orderBy("element_cume_dist")
w6 = Window.orderBy(F.col("element_cume_dist").asc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("element"),
    )
)

base = (
    lines
    .filter(~F.col("plan").isin(["pro", "basic", "enterprise"]))
    .select("*", F.lower("plan").alias("plan_lower"))
)

valid = (
    base
    .filter(F.col("plan").isin(["pro", "basic", "enterprise"]))
    .select("*", F.lower("plan").alias("plan_lower_2"))
)

enriched = (
    valid
    .filter(F.col("plan") == "basic")
    .select("id", "effective_from", F.cume_dist().over(w).alias("element_cume_dist"))
)

scored = enriched.select("*", F.rank().over(w_hist).alias("element_cume_dist_rank"))

metrics = (
    scored
    .withColumn("element_cume_dist_cume_dist", F.cume_dist().over(w3))
    .withColumn("element_cume_dist_rank_rank", F.rank().over(w4))
    .withColumn("element_cume_dist_percent_rank", F.percent_rank().over(w5))
    .withColumn("element_cume_dist_cume_dist_2", F.cume_dist().over(w6))
)

stats = (
    metrics
    .cube(
        F.date_trunc("YEAR", "effective_from").alias("year_start"),
        F.when(F.col("element_cume_dist_cume_dist_2") >= 0.87, "LARGE").otherwise("SMALL").alias("element_cume_dist_cume_dist_2_tier"),
        F.when(F.col("element_cume_dist_cume_dist") >= 0.7, "LARGE").otherwise("SMALL").alias("element_cume_dist_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("element_cume_dist_rank_rank", [0.25, 0.5, 0.75]).alias("element_cume_dist_rank_rank_quartiles"),
        F.percentile_approx("element_cume_dist_cume_dist", [0.25, 0.5, 0.75]).alias("element_cume_dist_cume_dist_quartiles"),
        F.percentile_approx("element_cume_dist_rank_rank", 0.5).alias("element_cume_dist_rank_rank_p50"),
    )
    .filter(F.col("element_cume_dist_rank_rank_p50") > 0)
)

result = (
    stats
    .select(
        "element_cume_dist_cume_dist_2_tier",
        "element_cume_dist_cume_dist_tier",
        "element_cume_dist_cume_dist_quartiles",
        "element_cume_dist_rank_rank_p50",
    )
)
