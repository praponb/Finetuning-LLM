from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("discount_pct", "id")
w4 = Window.orderBy("running_discount_pct", "id")
w5 = Window.orderBy(F.col("running_discount_pct_2").desc_nulls_last())

latest = (
    df_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "discount_pct")
)

enriched = (
    latest
    .filter(F.col("discount_pct") <= 0.23)
    .select(
        "id",
        "discount_pct",
        F.sum("discount_pct").over(w2).alias("running_discount_pct"),
        F.sum("discount_pct").over(w2).alias("running_discount_pct_2"),
        F.lag("discount_pct", 1, 0).over(w3).alias("prev_discount_pct"),
    )
)

cleaned = (
    enriched
    .filter(F.col("prev_discount_pct") < 0.3)
    .select(
        "*",
        F.when(F.col("discount_pct") >= 0.33, "High").when(F.col("discount_pct") >= 0.1, "Medium").otherwise("Low").alias("discount_pct_band"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "discount_pct",
        "discount_pct_band",
        F.lag("running_discount_pct_2", 1).over(w4).alias("prev_running_discount_pct_2"),
        F.rank().over(w5).alias("running_discount_pct_2_rank"),
    )
)

daily = (
    scored
    .groupBy(
        F.when(F.col("prev_running_discount_pct_2") >= 2, "HIGH").otherwise("LOW").alias("prev_running_discount_pct_2_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("prev_running_discount_pct_2").alias("avg_prev_running_discount_pct_2"),
        F.percentile_approx("running_discount_pct_2_rank", 0.5).alias("running_discount_pct_2_rank_p50"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
    )
)

result = (
    daily
    .select(
        "prev_running_discount_pct_2_tier",
        "distinct_ids",
        "avg_prev_running_discount_pct_2",
        "event_count",
        "distinct_ids_2",
        "avg_discount_pct",
    )
    .orderBy(F.col("distinct_ids").desc())
)
