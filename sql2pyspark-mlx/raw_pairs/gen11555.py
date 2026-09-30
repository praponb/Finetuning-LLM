from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w_grp = Window.partitionBy("currency").orderBy(F.col("plan_min_element").desc())
w3 = Window.partitionBy("currency", "plan").orderBy(F.col("plan_min_element").desc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("plan_min_element").desc())
w5 = Window.partitionBy("currency", "plan")
w6 = Window.partitionBy("currency")
w7 = Window.orderBy(F.col("plan_min_element_cume_dist").asc_nulls_last())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("element"))
)

metrics = (
    flattened
    .filter(F.col("plan") == "basic")
    .select(
        "id",
        "plan",
        "currency",
        "element",
        F.min("element").over(w).alias("plan_min_element"),
    )
)

metrics_price_tiers = (
    metrics
    .select(
        "id",
        "plan",
        "currency",
        F.rank().over(w_grp).alias("plan_min_element_rank"),
        F.cume_dist().over(w3).alias("plan_min_element_cume_dist"),
        F.percent_rank().over(w4).alias("plan_min_element_percent_rank"),
    )
)

metrics_price_tiers_2 = (
    metrics_price_tiers
    .withColumn("currency_min_plan_min_element_cume_dist", F.min("plan_min_element_cume_dist").over(w5))
    .withColumn("currency_sum_plan_min_element_percent_rank", F.sum("plan_min_element_percent_rank").over(w6))
    .withColumn("plan_min_element_cume_dist_percent_rank", F.percent_rank().over(w7))
)

base = (
    metrics_price_tiers_2
    .filter(F.col("currency_sum_plan_min_element_percent_rank") >= 3)
    .select(
        "id",
        "plan",
        "currency",
        "plan_min_element_rank",
        "plan_min_element_cume_dist",
        "currency_min_plan_min_element_cume_dist",
        "plan_min_element_cume_dist_percent_rank",
        F.least("currency_min_plan_min_element_cume_dist", "plan_min_element_cume_dist").alias("least_currency_min_plan_min_element_cume_dist_plan_min_element_cume_dist"),
        (F.col("plan_min_element_percent_rank") - F.col("plan_min_element_cume_dist")).alias("plan_min_element_percent_rank_minus_plan_min_element_cume_dist"),
    )
)

result = (
    base
    .select(
        "plan",
        "currency",
        "plan_min_element_rank",
        "plan_min_element_cume_dist",
        "plan_min_element_cume_dist_percent_rank",
        "least_currency_min_plan_min_element_cume_dist_plan_min_element_cume_dist",
        "plan_min_element_percent_rank_minus_plan_min_element_cume_dist",
    )
    .orderBy("plan_min_element_cume_dist_percent_rank")
)
