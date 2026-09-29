from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("currency", "plan")
w_hist = Window.partitionBy("plan").orderBy("element")
w3 = Window.partitionBy("currency").orderBy("element")
w4 = Window.partitionBy("currency").orderBy(F.col("greatest_currency_min_element_element").asc_nulls_first())
w5 = Window.orderBy(F.col("greatest_currency_min_element_element").desc())
w6 = Window.partitionBy("plan", "currency").orderBy(F.col("element").desc())
w7 = Window.orderBy(F.col("element_cume_dist_plus_element_cume_dist_2").desc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("element"))
)

with_windows = (
    exploded
    .select(
        "*",
        F.min("element").over(w_ordered).alias("currency_min_element"),
        F.cume_dist().over(w_hist).alias("element_cume_dist"),
        F.cume_dist().over(w3).alias("element_cume_dist_2"),
    )
)

base = (
    with_windows
    .filter(F.col("element") <= 28)
    .select(
        "id",
        "plan",
        "currency",
        "element",
        "currency_min_element",
        "element_cume_dist",
        "element_cume_dist_2",
        F.greatest("currency_min_element", "element").alias("greatest_currency_min_element_element"),
        (F.col("element_cume_dist") + F.col("element_cume_dist_2")).alias("element_cume_dist_plus_element_cume_dist_2"),
    )
)

with_windows_price_tiers = (
    base
    .withColumn("greatest_currency_min_element_element_cume_dist", F.cume_dist().over(w4))
)

with_windows_price_tiers_2 = (
    with_windows_price_tiers
    .filter(F.col("currency_min_element") < 71)
    .withColumn("greatest_currency_min_element_element_rank", F.rank().over(w5))
    .withColumn("element_cume_dist_3", F.cume_dist().over(w6))
    .withColumn("element_cume_dist_plus_element_cume_dist_2_cume_dist", F.cume_dist().over(w7))
)

result = (
    with_windows_price_tiers_2
    .groupBy("currency")
    .pivot("plan", ["pro", "basic"])
    .agg(
        F.sum("element_cume_dist").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
