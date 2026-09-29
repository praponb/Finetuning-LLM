from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("created_at"))
w2 = Window.orderBy(F.col("price_rounded").desc_nulls_last())
w3 = Window.partitionBy(F.month("created_at")).orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

lines = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select("id", "created_at", "coupon_codes", F.col("li_item.price"))
)

cleaned = (
    lines
    .filter(F.to_date("created_at") >= "2023-07-20")
    .withColumn("price_filled", F.coalesce("price", F.lit(0)))
    .withColumn("price_rounded", F.round("price", 0))
)

cleaned_carts = (
    cleaned
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .select(
        "created_at",
        "price_rounded",
        F.round("price_filled", 0).alias("price_filled_rounded"),
    )
)

enriched = (
    cleaned_carts
    .filter(F.col("price_rounded") <= 36)
    .select(
        "*",
        F.avg("price_rounded").over(w).alias("created_at_year_avg_price_rounded"),
        F.cume_dist().over(w2).alias("price_rounded_cume_dist"),
        F.avg("price_filled_rounded").over(w3).alias("price_filled_rounded_avg_30m"),
    )
)

cleaned_carts_2 = (
    enriched
    .filter(F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 15:00:00")))
    .select(
        "price_rounded",
        "created_at_year_avg_price_rounded",
        "price_rounded_cume_dist",
        "price_filled_rounded_avg_30m",
        F.quarter("created_at").alias("created_at_quarter"),
    )
)

result = (
    cleaned_carts_2
    .groupBy(
        F.when(F.col("price_rounded_cume_dist") >= 0.83, "LARGE").otherwise("SMALL").alias("price_rounded_cume_dist_tier"),
        F.when(F.col("created_at_year_avg_price_rounded") >= 80, "PREMIUM").otherwise("STANDARD").alias("created_at_year_avg_price_rounded_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max("created_at_quarter").alias("max_created_at_quarter"),
        F.round(F.avg("price_rounded"), 2).alias("avg_price_rounded"),
        F.count("*").alias("event_count"),
        F.percentile_approx("created_at_quarter", [0.25, 0.5, 0.75]).alias("created_at_quarter_quartiles"),
    )
    .filter(F.col("cnt") > 1)
)
