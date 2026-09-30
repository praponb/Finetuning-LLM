from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("element").desc())
w3 = Window.partitionBy("currency").orderBy(F.col("currency_avg_element").asc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

metrics = (
    exploded
    .withColumn("currency_sum_element", F.sum("element").over(w))
    .withColumn("element_dense_rank", F.dense_rank().over(w2))
    .withColumn("currency_avg_element", F.avg("element").over(w))
)

scored = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w3).alias("currency_avg_element_cume_dist"),
        F.percent_rank().over(w4).alias("element_percent_rank"),
        F.round((F.col("element_dense_rank") / F.sum("element_dense_rank").over(w)), 4).alias("element_dense_rank_share"),
    )
)

valid = (
    scored
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-05-23")))
    .select(
        "*",
        (F.col("currency_sum_element") + F.col("element_dense_rank")).alias("currency_sum_element_plus_element_dense_rank"),
    )
)

result = (
    valid
    .select(
        "id",
        "element_dense_rank",
        "currency_avg_element_cume_dist",
        "element_percent_rank",
        "currency_sum_element_plus_element_dense_rank",
    )
    .orderBy(
        F.col("currency_sum_element_plus_element_dense_rank").asc_nulls_last(),
        F.col("element_dense_rank").desc(),
    )
)
