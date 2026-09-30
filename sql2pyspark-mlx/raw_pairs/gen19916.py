from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("element").desc_nulls_first())
w3 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())
w4 = Window.orderBy("plan_min_element")
w5 = Window.orderBy(F.col("plan_min_element").asc_nulls_last())

lines = df_price_tiers.select("id", "plan", F.explode("discounts").alias("element"))

base = (
    lines
    .filter(F.col("plan").isin(["enterprise", "starter", "basic"]))
    .withColumn("clean_plan", F.trim("plan"))
)

with_windows = (
    base
    .filter(F.col("element").between(27, 51))
    .select(
        "id",
        "clean_plan",
        F.min("element").over(w).alias("plan_min_element"),
        F.rank().over(w2).alias("element_rank"),
        F.sum("element").over(w).alias("plan_sum_element"),
        F.cume_dist().over(w3).alias("element_cume_dist"),
        F.avg("element").over(w).alias("plan_avg_element"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "clean_plan",
        "plan_min_element",
        "plan_avg_element",
        F.percent_rank().over(w4).alias("plan_min_element_percent_rank"),
        F.percent_rank().over(w5).alias("plan_min_element_percent_rank_2"),
    )
)

result = (
    metrics
    .select(
        "clean_plan",
        "plan_min_element",
        "plan_avg_element",
        "plan_min_element_percent_rank",
        "plan_min_element_percent_rank_2",
    )
    .orderBy(
        F.col("plan_min_element_percent_rank").asc_nulls_first(),
        "plan_min_element_percent_rank_2",
    )
)
