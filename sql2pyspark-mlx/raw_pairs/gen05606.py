from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("plan", "currency").orderBy(F.col("element").desc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_last())
w_running = Window.partitionBy("plan", "currency")
w5 = Window.partitionBy(F.date_trunc("month", "effective_from_minus_38d")).orderBy("currency_max_element")
w_hist = Window.partitionBy("plan").orderBy(F.col("currency_max_element").desc())

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

scored = (
    lines
    .withColumn("currency_max_element", F.max("element").over(w))
    .withColumn("element_percent_rank", F.percent_rank().over(w2))
    .withColumn("element_dense_rank", F.dense_rank().over(w3))
)

matched_keys = (
    scored
    .select("plan", "currency")
    .exceptAll(
        df_price_tiers
        .filter(F.col("plan") == "pro")
        .select("plan", "currency")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("effective_from") > F.lit("2024-02-10").cast("date"))
    .select(
        "plan",
        "currency",
        "element",
        "currency_max_element",
        "element_percent_rank",
        "element_dense_rank",
        F.date_sub("effective_from", 38).alias("effective_from_minus_38d"),
    )
)

scored_price_tiers = (
    valid
    .select(
        "*",
        F.max("currency_max_element").over(w_running).alias("plan_max_currency_max_element"),
        F.avg("element").over(w_running).alias("plan_avg_element"),
        F.percent_rank().over(w5).alias("currency_max_element_percent_rank"),
        F.percent_rank().over(w_hist).alias("currency_max_element_percent_rank_2"),
    )
)

result = (
    scored_price_tiers
    .select(
        "plan",
        "element",
        "currency_max_element",
        "element_percent_rank",
        "element_dense_rank",
        "effective_from_minus_38d",
        "plan_avg_element",
        "currency_max_element_percent_rank",
        "currency_max_element_percent_rank_2",
    )
    .orderBy(F.col("currency_max_element").desc())
)
