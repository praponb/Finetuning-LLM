from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy(F.date_trunc("YEAR", "effective_from")).orderBy(F.col("adjusted_currency_sum_element").asc_nulls_last())
w3 = Window.orderBy(F.col("currency_sum_element").asc_nulls_last())
w4 = Window.orderBy(F.col("adjusted_currency_sum_element_int").desc())
w5 = Window.orderBy(F.col("currency_sum_element").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

matched_keys = (
    lines
    .select("currency")
    .subtract(
        df_price_tiers
        .filter(F.col("currency") == "USD")
        .select("currency")
    )
)

verified = (
    lines
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "id",
        "effective_from",
        F.sum("element").over(w).alias("currency_sum_element"),
    )
)

valid = (
    scored
    .filter(F.col("currency_sum_element").between(248, 297))
    .withColumn("adjusted_currency_sum_element", F.round((F.col("currency_sum_element") * 0.9), 2))
    .withColumn("currency_sum_element_str", F.col("currency_sum_element").cast("string"))
)

filtered = (
    valid
    .filter((F.col("effective_from") > F.lit("2024-06-19").cast("date")) & (F.col("adjusted_currency_sum_element") < 47))
    .withColumn("adjusted_currency_sum_element_int", F.col("adjusted_currency_sum_element").cast("int"))
)

with_windows = (
    filtered
    .filter(F.col("currency_sum_element") <= 231)
    .select(
        "id",
        "effective_from",
        F.percent_rank().over(w2).alias("adjusted_currency_sum_element_percent_rank"),
        F.rank().over(w3).alias("currency_sum_element_rank"),
        F.cume_dist().over(w4).alias("adjusted_currency_sum_element_int_cume_dist"),
        F.cume_dist().over(w5).alias("currency_sum_element_cume_dist"),
    )
)

filtered_price_tiers = (
    with_windows
    .filter(F.col("effective_from").between("2025-02-01", "2025-02-20"))
    .withColumn("currency_sum_element_cume_dist_int", F.col("currency_sum_element_cume_dist").cast("int"))
    .withColumn("adjusted_currency_sum_element_percent_rank_minus_adjusted_currency_sum_element_int_cume_dist", (F.col("adjusted_currency_sum_element_percent_rank") - F.col("adjusted_currency_sum_element_int_cume_dist")))
)

result = (
    filtered_price_tiers
    .select(
        "id",
        "adjusted_currency_sum_element_int_cume_dist",
        "currency_sum_element_cume_dist",
        "currency_sum_element_cume_dist_int",
        "adjusted_currency_sum_element_percent_rank_minus_adjusted_currency_sum_element_int_cume_dist",
    )
)
