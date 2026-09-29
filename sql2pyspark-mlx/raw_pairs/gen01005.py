from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan").orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.orderBy(F.col("element").asc_nulls_first())
w3 = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_first())
w_running = Window.partitionBy("plan")
w5 = Window.partitionBy(F.year("effective_from_period"), "plan")
w6 = Window.partitionBy("plan").orderBy(F.col("element_percent_rank").desc())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("element"),
    )
)

filtered = (
    exploded
    .filter(F.col("effective_from") > F.lit("2024-05-12").cast("date"))
    .select(
        "plan",
        "effective_from",
        "currency",
        "element",
        F.when(F.col("currency").isin(["USD", "THB"]), "Core").otherwise("Other").alias("currency_group"),
    )
)

cleaned = (
    filtered
    .filter(F.col("plan").isin(["basic", "enterprise", "starter"]))
    .withColumn("effective_from_period", F.date_trunc("year", "effective_from"))
    .withColumn("clean_plan", F.trim("plan"))
)

metrics = (
    cleaned
    .filter(F.to_date("effective_from_period") < "2023-03-01")
    .select(
        "plan",
        "element",
        "currency_group",
        "effective_from_period",
        "clean_plan",
        F.max("element").over(w_ordered).alias("element_max_30d"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
        F.dense_rank().over(w3).alias("element_dense_rank"),
    )
)

with_windows = (
    metrics
    .select(
        "element",
        "effective_from_period",
        "clean_plan",
        F.count("element_dense_rank").over(w_running).alias("plan_count_element_dense_rank"),
        F.sum("element").over(w5).alias("effective_from_period_year_sum_element"),
        F.rank().over(w6).alias("element_percent_rank_rank"),
    )
)

result = (
    with_windows
    .select(
        "element",
        "clean_plan",
        "plan_count_element_dense_rank",
        "effective_from_period_year_sum_element",
    )
    .orderBy(F.col("plan_count_element_dense_rank").desc())
)
