from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("element"))
)

valid = (
    lines
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-07-09")))
    .select("*", F.trim("plan").alias("clean_plan"))
)

scored = (
    valid
    .withColumn("plan_sum_element", F.sum("element").over(w))
    .withColumn("plan_sum_element_2", F.sum("element").over(w))
    .withColumn("element_percent_rank", F.percent_rank().over(w2))
)

remaining_keys = (
    scored
    .select("id")
    .intersect(
        df_price_tiers
        .filter(F.col("currency") == "USD")
        .select("id")
    )
)

verified = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

stats = (
    verified
    .groupBy("plan")
    .agg(
        F.min("plan_sum_element").alias("min_plan_sum_element"),
        F.sum(F.when(F.col("plan") == "pro", F.col("element_percent_rank")).otherwise(0)).alias("pro_element_percent_rank"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.countDistinct("plan").alias("distinct_plans_2"),
        F.countDistinct("plan").alias("distinct_plans_3"),
    )
)

result = (
    stats
    .select(
        "plan",
        "min_plan_sum_element",
        "pro_element_percent_rank",
        "distinct_plans_2",
        "distinct_plans_3",
    )
    .orderBy("distinct_plans_2")
)
