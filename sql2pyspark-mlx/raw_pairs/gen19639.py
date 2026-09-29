from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("months_effective_from_to_effective_from_minus_72d").asc_nulls_last())
w_hist = Window.orderBy(F.col("months_effective_from_to_effective_from_minus_72d").desc_nulls_first())
w3 = Window.partitionBy(F.year("effective_from_minus_72d"))
w4 = Window.orderBy("val")

lines = df_price_tiers.select("id", "effective_from", F.explode("discounts").alias("val"))

filtered = (
    lines
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-06-05")))
    .withColumn("effective_from_minus_72d", F.date_sub("effective_from", 72))
)

cleaned = (
    filtered
    .filter((F.col("effective_from") >= F.lit("2024-03-19").cast("date")) & F.col("val").between(30, 43))
    .select(
        "*",
        F.round((F.datediff("effective_from_minus_72d", "effective_from") / 30), 1).alias("months_effective_from_to_effective_from_minus_72d"),
    )
)

scored = (
    cleaned
    .withColumn("months_effective_from_to_effective_from_minus_72d_cume_dist", F.cume_dist().over(w))
    .withColumn("months_effective_from_to_effective_from_minus_72d_rank", F.rank().over(w_hist))
)

scored_price_tiers = (
    scored
    .select(
        "val",
        "months_effective_from_to_effective_from_minus_72d",
        "months_effective_from_to_effective_from_minus_72d_cume_dist",
        F.sum("months_effective_from_to_effective_from_minus_72d").over(w3).alias("effective_from_minus_72d_year_sum_months_effective_from_to_effective_from_minus_72d"),
        F.percent_rank().over(w4).alias("val_percent_rank"),
    )
)

result = (
    scored_price_tiers
    .select(
        "val",
        "months_effective_from_to_effective_from_minus_72d",
        "months_effective_from_to_effective_from_minus_72d_cume_dist",
        "val_percent_rank",
    )
    .orderBy(F.col("months_effective_from_to_effective_from_minus_72d").desc())
)
