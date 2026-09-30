from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc())
w2 = Window.partitionBy("currency")

lines = df_price_tiers.select("id", "currency", F.explode("tier_prices").alias("element"))

scored = (
    lines
    .withColumn("element_percent_rank", F.percent_rank().over(w))
    .withColumn("currency_count_element", F.count("element").over(w2))
    .withColumn("currency_stddev_samp_element", F.stddev_samp("element").over(w2))
    .withColumn("currency_sum_element", F.sum("element").over(w2))
    .withColumn("currency_max_element", F.max("element").over(w2))
)

valid = (
    scored
    .filter(F.col("currency_sum_element") <= 250)
    .select(
        "*",
        (F.col("currency_max_element") + F.col("element_percent_rank")).alias("currency_max_element_plus_element_percent_rank"),
        F.when(F.col("currency_max_element") >= 76, "High").when(F.col("currency_max_element") >= 30, "Medium").otherwise("Low").alias("currency_max_element_band"),
    )
)

metrics = (
    valid
    .withColumn("currency_count_element_share", F.round((F.col("currency_count_element") / F.sum("currency_count_element").over(w2)), 4))
    .withColumn("currency_stddev_samp_currency_sum_element", F.stddev_samp("currency_sum_element").over(w2))
)

totals = (
    metrics
    .withColumn("element_tier", F.when(F.col("element") >= 76, "PREMIUM").otherwise("STANDARD"))
    .withColumn("currency_count_element_share_tier", F.when(F.col("currency_count_element_share") >= 0.71, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["element_tier", "currency_count_element_share_tier"], ["element_tier"]],
        "element_tier",
        "currency_count_element_share_tier",
    )
    .agg(
        F.count_if(F.col("currency") == "THB").alias("thb_events"),
        F.round(F.avg("currency_count_element_share"), 2).alias("avg_currency_count_element_share"),
        F.round(F.avg("currency_count_element_share"), 2).alias("avg_currency_count_element_share_2"),
        F.grouping("element_tier").alias("grouping_id"),
    )
)

result = totals.select("element_tier", "avg_currency_count_element_share_2", "grouping_id")
