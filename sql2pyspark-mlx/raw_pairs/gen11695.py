from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("years_since_effective_from").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

base = (
    lines
    .filter(F.col("effective_from") > F.lit("2024-02-02").cast("date"))
    .withColumn("years_since_effective_from", (F.datediff(F.current_date(), "effective_from") / 365))
)

with_windows = (
    base
    .withColumn("currency_count_tier_value", F.count("tier_value").over(w))
    .withColumn("years_since_effective_from_cume_dist", F.cume_dist().over(w2))
)

valid = (
    with_windows
    .filter(F.col("years_since_effective_from") >= 43)
    .select(
        "*",
        F.round((F.col("currency_count_tier_value") * 1.1), 2).alias("currency_count_tier_value_adjusted"),
    )
)

result = (
    valid
    .withColumn("activity_month", F.date_trunc("MONTH", "effective_from"))
    .groupingSets(
        [["currency", "activity_month"], ["currency"]],
        "currency",
        "activity_month",
    )
    .agg(
        F.max(F.when(F.col("currency") == "THB", F.col("years_since_effective_from"))).alias("thb_max_years_since_effective_from"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("years_since_effective_from")).otherwise(0)).alias("eur_years_since_effective_from"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
    )
    .filter(F.col("distinct_ids") > 1)
)
