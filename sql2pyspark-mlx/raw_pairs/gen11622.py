from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency")
w_hist = Window.partitionBy("plan")
w3 = Window.orderBy(F.col("plan_length").desc_nulls_first())
w4 = Window.orderBy(F.col("plan_count_element_2").desc_nulls_first())
w5 = Window.orderBy(F.col("plan_length").asc_nulls_last())
w6 = Window.orderBy(F.col("plan_count_element").desc())
w7 = Window.partitionBy("plan_count_element_tier").orderBy(F.col("row_count_2").desc())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("element"),
    )
)

base = (
    exploded
    .filter(F.col("plan") == "enterprise")
    .withColumn("plan_length", F.length("plan"))
)

scored = (
    base
    .select(
        "id",
        "plan_length",
        F.count("element").over(w).alias("plan_count_element"),
        F.count("plan_length").over(w_hist).alias("plan_count_plan_length"),
        F.count("element").over(w_hist).alias("plan_count_element_2"),
    )
)

with_windows = (
    scored
    .select(
        "plan_count_element",
        "plan_count_element_2",
        F.dense_rank().over(w3).alias("plan_length_dense_rank"),
        F.dense_rank().over(w4).alias("plan_count_element_2_dense_rank"),
        F.percent_rank().over(w5).alias("plan_length_percent_rank"),
        F.cume_dist().over(w6).alias("plan_count_element_cume_dist"),
    )
)

base_price_tiers = (
    with_windows
    .filter(F.col("plan_count_element_cume_dist") > 0.51)
    .withColumn("plan_count_element_cume_dist_whole", F.floor("plan_count_element_cume_dist"))
)

cleaned = (
    base_price_tiers
    .filter(F.col("plan_count_element_2_dense_rank") == 3)
    .select(
        "plan_count_element",
        "plan_count_element_2",
        "plan_count_element_2_dense_rank",
        "plan_length_percent_rank",
        "plan_count_element_cume_dist_whole",
        F.round("plan_count_element_cume_dist", 1).alias("plan_count_element_cume_dist_rounded"),
    )
)

base_price_tiers_2 = (
    cleaned
    .filter(F.col("plan_count_element").isNotNull())
    .withColumn("plan_count_element_minus_plan_count_element_2", (F.col("plan_count_element") - F.col("plan_count_element_2")))
)

result = (
    base_price_tiers_2
    .groupBy(
        F.when(F.col("plan_count_element") >= 15, "LARGE").otherwise("SMALL").alias("plan_count_element_tier"),
        F.when(F.col("plan_count_element_2_dense_rank") >= 7, "LARGE").otherwise("SMALL").alias("plan_count_element_2_dense_rank_tier"),
    )
    .agg(
        F.round(F.avg("plan_count_element_2"), 2).alias("avg_plan_count_element_2"),
        F.count("*").alias("row_count"),
        F.count("*").alias("row_count_2"),
        F.avg("plan_length_percent_rank").alias("avg_plan_length_percent_rank"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("plan_count_element_2", 0.9).alias("plan_count_element_2_p90"),
    )
    .withColumn("row_count_2_percent_rank", F.percent_rank().over(w7))
    .orderBy(F.col("row_count").desc())
)
