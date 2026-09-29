from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("val").desc())
w3 = Window.partitionBy("plan")

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("discounts").alias("val"))
)

cleaned = (
    lines
    .filter(F.year("effective_from") >= 2025)
    .withColumn("effective_from_month_end", F.last_day("effective_from"))
)

base = (
    cleaned
    .filter(F.col("effective_from") >= "2023-06-18")
    .withColumn("effective_from_month_end_plus_65d", F.date_add("effective_from_month_end", 65))
    .withColumn("is_enterprise", F.when(F.col("plan") == "enterprise", 1).otherwise(0))
)

with_windows = (
    base
    .filter(F.col("effective_from_month_end_plus_65d") > "2023-09-22")
    .select(
        "plan",
        "effective_from",
        "val",
        "effective_from_month_end_plus_65d",
        "is_enterprise",
        F.dense_rank().over(w).alias("val_dense_rank"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.sum("val").over(w3).alias("plan_sum_val"),
        F.sum("val").over(w3).alias("plan_sum_val_2"),
    )
)

result = (
    metrics
    .select(
        "plan",
        "val",
        "effective_from_month_end_plus_65d",
        "is_enterprise",
        "val_percent_rank",
        "plan_sum_val",
        "plan_sum_val_2",
    )
)
