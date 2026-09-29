from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy(F.col("val").asc_nulls_last())
w2 = Window.orderBy(F.col("val_rank").asc_nulls_first())
w3 = Window.partitionBy(F.year("effective_from"))
w4 = Window.orderBy(F.col("val_rank").desc_nulls_first())
w5 = Window.orderBy("val_rank")

flattened = df_price_tiers.select("id", "effective_from", F.explode("discounts").alias("val"))

valid = (
    flattened
    .filter(F.col("effective_from") > "2025-04-22")
    .withColumn("effective_from_plus_months", F.add_months("effective_from", 3))
)

metrics = (
    valid
    .filter(F.col("val").between(28, 59))
    .select("id", "effective_from", F.rank().over(w_ordered).alias("val_rank"))
)

scored = (
    metrics
    .select(
        "id",
        "effective_from",
        F.percent_rank().over(w2).alias("val_rank_percent_rank"),
        F.avg("val_rank").over(w3).alias("effective_from_year_avg_val_rank"),
        F.sum("val_rank").over(w3).alias("effective_from_year_sum_val_rank"),
        F.percent_rank().over(w4).alias("val_rank_percent_rank_2"),
        F.rank().over(w5).alias("val_rank_rank"),
    )
)

base = (
    scored
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-04-22")))
    .select(
        "id",
        "effective_from",
        "effective_from_year_avg_val_rank",
        "val_rank_percent_rank_2",
        "val_rank_rank",
        F.date_trunc("year", "effective_from").alias("effective_from_period"),
    )
)

result = (
    base
    .select("id", "effective_from_year_avg_val_rank", "val_rank_percent_rank_2")
    .orderBy(F.col("effective_from_year_avg_val_rank").desc(), "id")
)
