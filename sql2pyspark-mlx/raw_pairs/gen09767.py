from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("element").desc())
w3 = Window.orderBy(F.col("element").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("element"))
)

base = (
    flattened
    .filter(F.col("element") <= 40)
    .select("*", F.concat_ws("-", "plan", "currency").alias("plan_currency_key"))
)

base_price_tiers = (
    base
    .filter(F.col("currency") != "USD")
    .select(
        "plan",
        "currency",
        "element",
        "plan_currency_key",
        F.substring("currency", 1, 3).alias("currency_prefix"),
        F.substring("currency", 1, 3).alias("currency_prefix_2"),
    )
)

metrics = (
    base_price_tiers
    .filter(F.col("plan") == "pro")
    .select(
        "*",
        F.max("element").over(w).alias("plan_max_element"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
        F.cume_dist().over(w3).alias("element_cume_dist"),
    )
)

valid = (
    metrics
    .filter(F.col("plan").isin(["pro", "enterprise"]))
    .select(
        "*",
        F.coalesce("element_cume_dist", F.lit(0)).alias("element_cume_dist_filled"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("element_percent_rank") >= 0.8, "LARGE").otherwise("SMALL").alias("element_percent_rank_tier"),
    )
    .agg(
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("element_cume_dist")).otherwise(0)).alias("eur_element_cume_dist"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
    )
    .orderBy(F.col("num_currencies").asc_nulls_last())
)
