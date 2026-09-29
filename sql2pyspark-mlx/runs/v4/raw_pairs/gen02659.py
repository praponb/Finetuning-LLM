from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan")
w2 = Window.orderBy("plan_avg_element_2")
w3 = Window.orderBy(F.col("element").desc_nulls_last())
w4 = Window.orderBy(F.col("plan_avg_element_times_plan_avg_element_2").desc_nulls_last())
w_hist = Window.partitionBy("plan").orderBy(F.col("plan_avg_element_scaled").desc())
w6 = Window.orderBy(F.col("element").desc_nulls_first())
w7 = Window.orderBy("plan_avg_element_times_plan_avg_element_2_percent_rank")
w8 = Window.partitionBy("plan").orderBy(F.col("plan_avg_element_times_plan_avg_element_2_percent_rank").desc_nulls_first())
w9 = Window.partitionBy("plan").orderBy(F.col("plan_max_element").desc())
w10 = Window.orderBy("plan_avg_element")

flattened = df_price_tiers.select("id", "plan", F.explode("tier_limits").alias("element"))

with_windows = (
    flattened
    .select(
        "*",
        F.avg("element").over(w_recent).alias("plan_avg_element"),
        F.avg("element").over(w_recent).alias("plan_avg_element_2"),
        F.sum("element").over(w_recent).alias("plan_sum_element"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w2).alias("plan_avg_element_2_cume_dist"),
        F.max("element").over(w_recent).alias("plan_max_element"),
    )
)

valid = (
    enriched
    .filter(F.col("plan_avg_element") >= 61)
    .select(
        "*",
        F.when(F.col("plan_avg_element_2_cume_dist") >= 0.88, "High").when(F.col("plan_avg_element_2_cume_dist") >= 0.39, "Medium").otherwise("Low").alias("plan_avg_element_2_cume_dist_band"),
    )
)

cleaned = (
    valid
    .filter(F.col("plan_avg_element") >= 77)
    .withColumn("plan_avg_element_times_plan_avg_element_2", (F.col("plan_avg_element") * F.col("plan_avg_element_2")))
    .withColumn("plan_avg_element_scaled", F.round((F.col("plan_avg_element") * 1.15), 2))
)

enriched_price_tiers = (
    cleaned
    .select(
        "*",
        F.rank().over(w3).alias("element_rank"),
        F.percent_rank().over(w4).alias("plan_avg_element_times_plan_avg_element_2_percent_rank"),
        F.dense_rank().over(w_hist).alias("plan_avg_element_scaled_dense_rank"),
        F.min("plan_max_element").over(w_recent).alias("plan_min_plan_max_element"),
        F.cume_dist().over(w6).alias("element_cume_dist"),
    )
)

scored = (
    enriched_price_tiers
    .select(
        "*",
        F.rank().over(w7).alias("plan_avg_element_times_plan_avg_element_2_percent_rank_rank"),
        F.rank().over(w8).alias("plan_avg_element_times_plan_avg_element_2_percent_rank_rank_2"),
        F.rank().over(w9).alias("plan_max_element_rank"),
        F.percent_rank().over(w10).alias("plan_avg_element_percent_rank"),
        F.max("element_cume_dist").over(w_recent).alias("plan_max_element_cume_dist"),
    )
)

result = (
    scored
    .cube(
        F.when(F.col("element_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("element_rank_tier"),
        "plan",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .filter(F.col("num_rows") >= 1)
)
