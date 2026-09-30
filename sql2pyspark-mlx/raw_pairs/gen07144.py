from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan", "currency")
w2 = Window.partitionBy("currency")
w_running = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w4 = Window.partitionBy("currency").orderBy(F.col("val_int").asc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("val"))
)

filtered = (
    lines
    .filter(F.col("val").between(33, 39))
    .select("*", F.coalesce("val", F.lit(0)).alias("val_filled"))
)

filtered_price_tiers = (
    filtered
    .filter(F.col("val_filled") > 46)
    .withColumn("val_int", F.col("val").cast("int"))
)

metrics = (
    filtered_price_tiers
    .withColumn("plan_max_val_filled", F.max("val_filled").over(w_hist))
    .withColumn("currency_sum_val_filled", F.sum("val_filled").over(w2))
    .withColumn("val_rank", F.rank().over(w_running))
    .withColumn("val_int_percent_rank", F.percent_rank().over(w4))
)

daily = (
    metrics
    .groupBy("plan")
    .agg(
        F.min("val_rank").alias("min_val_rank"),
        F.count("*").alias("row_count"),
        F.percentile_approx("val_rank", 0.5).alias("val_rank_p50"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.percentile_approx("val", 0.5).alias("val_p50"),
    )
)

result = (
    daily
    .select(
        "plan",
        "row_count",
        "distinct_currencies",
        "val_p50",
        F.when(F.col("plan").rlike("^(basic|enterprise)$"), "OTHER_ZONE").when(F.col("plan").rlike("^(starter|pro)$"), "EXTENDED").otherwise("PRIORITY").alias("plan_zone"),
    )
)
