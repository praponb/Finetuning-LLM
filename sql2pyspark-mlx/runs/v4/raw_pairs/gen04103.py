from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_last())
w_recent = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_first())
w3 = Window.partitionBy("plan").orderBy("element")
w4 = Window.partitionBy("plan", "currency")
w5 = Window.partitionBy("plan")
w6 = Window.orderBy(F.col("plan_min_element").desc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

with_windows = (
    lines
    .withColumn("element_percent_rank", F.percent_rank().over(w))
    .withColumn("element_cume_dist", F.cume_dist().over(w_recent))
    .withColumn("element_rank", F.rank().over(w3))
    .withColumn("plan_min_element", F.min("element").over(w4))
)

metrics = (
    with_windows
    .select(
        "*",
        F.min("element_rank").over(w5).alias("plan_min_element_rank"),
        F.count("element_cume_dist").over(w4).alias("plan_count_element_cume_dist"),
        F.rank().over(w6).alias("plan_min_element_rank_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "plan",
        "currency",
        "element",
        "element_percent_rank",
        "plan_min_element",
        "plan_min_element_rank",
        "plan_min_element_rank_2",
        F.when(F.col("currency").rlike("^(USD|EUR)$"), "EXTENDED").when(F.col("currency").rlike("^(THB)$"), "GROUP_B").otherwise("PRIORITY").alias("currency_zone"),
    )
)
