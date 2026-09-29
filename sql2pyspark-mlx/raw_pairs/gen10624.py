from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("element_percent_rank").desc_nulls_last())
w_recent = Window.partitionBy("currency").orderBy(F.col("element_percent_rank").asc_nulls_first())
w5 = Window.partitionBy("currency").orderBy(F.col("element_percent_rank").asc_nulls_last())
w6 = Window.partitionBy("currency").orderBy("element_percent_rank_rank")

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("discounts").alias("element"))
)

metrics = (
    exploded
    .filter(F.col("currency") != "EUR")
    .select(
        "id",
        "currency",
        F.percent_rank().over(w).alias("element_percent_rank"),
        F.avg("element").over(w2).alias("currency_avg_element"),
    )
)

valid = (
    metrics
    .filter(F.col("currency") == "USD")
    .withColumn("currency_avg_element_adjusted", F.round((F.col("currency_avg_element") * 0.85), 2))
)

enriched = (
    valid
    .select(
        "*",
        F.rank().over(w3).alias("element_percent_rank_rank"),
        F.cume_dist().over(w_recent).alias("element_percent_rank_cume_dist"),
        F.stddev_samp("currency_avg_element").over(w2).alias("currency_stddev_samp_currency_avg_element"),
    )
)

result = (
    enriched
    .select(
        "id",
        "element_percent_rank",
        "currency_avg_element",
        "element_percent_rank_cume_dist",
        F.rank().over(w5).alias("element_percent_rank_rank_2"),
        F.percent_rank().over(w6).alias("element_percent_rank_rank_percent_rank"),
    )
)
