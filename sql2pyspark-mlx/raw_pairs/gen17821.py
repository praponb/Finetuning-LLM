from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("element")
w2 = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w3 = Window.partitionBy("currency")
w4 = Window.partitionBy("currency").orderBy(F.col("currency_sum_element_2").desc_nulls_last())
w_running = Window.partitionBy("currency").orderBy(F.col("element_percent_rank").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("discounts").alias("element"))
)

matched_keys = (
    flattened
    .select("currency")
    .exceptAll(
        df_price_tiers
        .filter(F.col("currency") == "USD")
        .select("currency")
    )
)

reconciled = (
    flattened
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .withColumn("element_percent_rank", F.percent_rank().over(w))
    .withColumn("element_rank", F.rank().over(w2))
    .withColumn("currency_sum_element", F.sum("element").over(w3))
    .withColumn("currency_sum_element_2", F.sum("element").over(w3))
)

scored = (
    enriched
    .select(
        "id",
        "currency",
        "element",
        "element_percent_rank",
        "currency_sum_element",
        F.cume_dist().over(w4).alias("currency_sum_element_2_cume_dist"),
        F.sum("currency_sum_element").over(w3).alias("currency_sum_currency_sum_element"),
        F.round((F.col("element_rank") / F.sum("element_rank").over(w3)), 4).alias("element_rank_share"),
        F.percent_rank().over(w_running).alias("element_percent_rank_percent_rank"),
        F.stddev_samp("currency_sum_element").over(w3).alias("currency_stddev_samp_currency_sum_element"),
    )
)

result = (
    scored
    .select(
        "id",
        "element",
        "element_percent_rank",
        "currency_sum_element",
        "currency_sum_element_2_cume_dist",
        "currency_sum_currency_sum_element",
        "element_percent_rank_percent_rank",
    )
    .orderBy(
        F.col("element_percent_rank").desc_nulls_first(),
        F.col("currency_sum_element").desc(),
    )
)
