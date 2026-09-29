from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("value")
w2 = Window.orderBy(F.col("created_at_hour").desc_nulls_first())

lines = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("value"))

base = (
    lines
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .select("*", F.coalesce("value", F.lit("Unknown")).alias("value_filled"))
)

base_carts = (
    base
    .filter(F.col("created_at") >= "2023-04-01")
    .select("*", F.hour("created_at").alias("created_at_hour"))
)

valid = (
    base_carts
    .filter(F.col("value").isNotNull())
    .select(
        "id",
        "value",
        "value_filled",
        "created_at_hour",
        F.date_format("created_at", "yyyy-MM").alias("created_at_label"),
        (F.datediff(F.current_date(), "created_at") / 30).alias("months_since_created_at"),
        F.coalesce("value", F.lit("N/A")).alias("value_filled_2"),
    )
)

scored = (
    valid
    .select(
        "id",
        "value_filled",
        "created_at_hour",
        "value_filled_2",
        F.min("created_at_hour").over(w_running).alias("value_min_created_at_hour"),
        F.min("created_at_hour").over(w_running).alias("value_min_created_at_hour_2"),
        F.min("months_since_created_at").over(w_running).alias("value_min_months_since_created_at"),
        F.rank().over(w2).alias("created_at_hour_rank"),
    )
)

totals = (
    scored
    .groupBy(
        F.when(F.col("created_at_hour_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("created_at_hour_rank_tier"),
    )
    .agg(
        F.percentile_approx("value_min_months_since_created_at", [0.25, 0.5, 0.75]).alias("value_min_months_since_created_at_quartiles"),
        F.count("*").alias("event_count"),
        F.percentile_approx("created_at_hour_rank", [0.25, 0.5, 0.75]).alias("created_at_hour_rank_quartiles"),
    )
    .filter(F.col("event_count") > 1)
)

result = (
    totals
    .select(
        "created_at_hour_rank_tier",
        "value_min_months_since_created_at_quartiles",
        "event_count",
    )
    .orderBy(F.col("event_count").desc(), F.col("created_at_hour_rank_tier").desc())
)
