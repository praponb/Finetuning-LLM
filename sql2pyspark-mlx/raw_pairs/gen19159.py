from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc_nulls_last())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("val"),
    )
)

scored = (
    lines
    .select(
        "id",
        "currency",
        F.percent_rank().over(w).alias("val_percent_rank"),
        F.stddev_samp("val").over(w2).alias("plan_stddev_samp_val"),
        F.count("val").over(w3).alias("currency_count_val"),
    )
)

filtered = (
    scored
    .filter(F.col("plan_stddev_samp_val").between(72, 73))
    .withColumn("currency_upper", F.upper("currency"))
    .withColumn("val_percent_rank_int", F.col("val_percent_rank").cast("int"))
)

with_windows = (
    filtered
    .withColumn("currency_count_plan_stddev_samp_val", F.count("plan_stddev_samp_val").over(w3))
    .withColumn("currency_count_val_share", F.round((F.col("currency_count_val") / F.sum("currency_count_val").over(w3)), 4))
)

result = (
    with_windows
    .groupBy("currency")
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.percentile_approx("currency_count_plan_stddev_samp_val", 0.5).alias("currency_count_plan_stddev_samp_val_p50"),
    )
)
