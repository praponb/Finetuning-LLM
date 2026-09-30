from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("effective_from"), "plan")
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("effective_from_month_max_val").desc())

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_prices").alias("val"))
)

metrics = (
    exploded
    .select(
        "*",
        F.max("val").over(w).alias("effective_from_month_max_val"),
        F.sum("val").over(w2).alias("plan_sum_val"),
    )
)

metrics_price_tiers = (
    metrics
    .select(
        "id",
        "plan_sum_val",
        F.rank().over(w3).alias("effective_from_month_max_val_rank"),
    )
)

base = (
    metrics_price_tiers
    .filter(F.col("effective_from_month_max_val_rank").between(3, 5))
    .withColumn("plan_sum_val_minus_effective_from_month_max_val_rank", (F.col("plan_sum_val") - F.col("effective_from_month_max_val_rank")))
)

valid = (
    base
    .filter(F.col("plan_sum_val") <= 305)
    .withColumn("plan_sum_val_tier", F.when(F.col("plan_sum_val") >= 316, "High").when(F.col("plan_sum_val") >= 169, "Medium").otherwise("Low"))
)

result = valid.select("id", "plan_sum_val", "effective_from_month_max_val_rank")
