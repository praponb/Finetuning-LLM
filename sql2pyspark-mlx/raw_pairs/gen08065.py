from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan").orderBy(F.col("element").asc_nulls_first())
w2 = Window.orderBy("element")
w3 = Window.partitionBy("plan").orderBy(F.col("element_dense_rank_plus_element").asc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("element_dense_rank").desc_nulls_first())
w5 = Window.partitionBy("plan", "currency")
w6 = Window.orderBy(F.col("element").desc())
w_grp = Window.partitionBy("plan", "currency").orderBy(F.col("element_dense_rank_plus_element").desc_nulls_last())
w8 = Window.partitionBy("currency", "plan")
w9 = Window.orderBy(F.col("plan_min_element").asc_nulls_first())
w10 = Window.partitionBy("currency")
w11 = Window.partitionBy("currency", "plan").orderBy(F.col("element_rank").asc_nulls_last())
w_part = Window.partitionBy("plan")

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("element"))
)

enriched = exploded.withColumn("element_dense_rank", F.dense_rank().over(w))

valid = (
    enriched
    .filter(~F.col("currency").isin(["THB", "EUR"]))
    .select(
        "*",
        (F.col("element_dense_rank") + F.col("element")).alias("element_dense_rank_plus_element"),
    )
)

cleaned = (
    valid
    .filter(F.col("element_dense_rank_plus_element") <= 68)
    .withColumn("plan_currency_key", F.concat_ws("-", "plan", "currency"))
    .withColumn("clean_plan", F.trim("plan"))
)

metrics = (
    cleaned
    .filter(F.col("plan").isin(["enterprise", "starter", "pro"]))
    .withColumn("element_rank", F.rank().over(w2))
    .withColumn("element_dense_rank_plus_element_rank", F.rank().over(w3))
    .withColumn("element_dense_rank_dense_rank", F.dense_rank().over(w4))
    .withColumn("plan_min_element", F.min("element").over(w5))
    .withColumn("element_dense_rank_2", F.dense_rank().over(w6))
)

enriched_price_tiers = (
    metrics
    .select(
        "*",
        F.rank().over(w_grp).alias("element_dense_rank_plus_element_rank_2"),
        F.round((F.col("element_dense_rank") / F.sum("element_dense_rank").over(w8)), 4).alias("element_dense_rank_share"),
        F.cume_dist().over(w9).alias("plan_min_element_cume_dist"),
        F.avg("element_dense_rank_plus_element").over(w10).alias("currency_avg_element_dense_rank_plus_element"),
        F.rank().over(w11).alias("element_rank_rank"),
    )
)

with_windows = (
    enriched_price_tiers
    .select(
        "id",
        "currency",
        "element_dense_rank",
        "element_dense_rank_plus_element",
        "plan_currency_key",
        "element_dense_rank_plus_element_rank",
        "plan_min_element",
        "element_dense_rank_2",
        "plan_min_element_cume_dist",
        F.min("element_dense_rank_plus_element").over(w_part).alias("plan_min_element_dense_rank_plus_element"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "currency",
        "element_dense_rank",
        "element_dense_rank_plus_element",
        "element_dense_rank_plus_element_rank",
        "plan_min_element",
        "element_dense_rank_2",
        "plan_min_element_cume_dist",
        "plan_min_element_dense_rank_plus_element",
        F.when(F.col("currency").rlike("^(THB|USD)$"), "CORE").otherwise("PRIORITY").alias("currency_zone"),
    )
    .orderBy(F.col("element_dense_rank_2").desc_nulls_first())
)
