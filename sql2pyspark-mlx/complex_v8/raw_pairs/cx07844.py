from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_status = Window.partitionBy(F.date_trunc("month", "start_date"), "status")
w_holder_id_by_premium_filled = Window.partitionBy("holder_id").orderBy("premium_filled_per_premium", "id")

cleaned_policys = (
    df_policies
    .filter((F.col("product").isNotNull()) & (F.col("end_date") >= F.lit("2026-02-26").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
        F.when(F.col("premium") >= 3532.7, "A").when(F.col("premium") >= 2289.0, "B").otherwise("C").alias("premium_tier"),
    )
)

tagged = (
    cleaned_policys
    .filter(F.col("holder_id") >= 6)
    .select(
        "*",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.concat(F.col("premium_tier"), F.lit("#"), F.col("holder_id").cast("string")).alias("premium_tier_ref"),
    )
)

enriched = (
    tagged
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        "premium_filled",
        F.round(F.col("premium_filled") / F.nullif(F.col("premium"), F.lit(0)), 3).alias("premium_filled_per_premium"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.concat(F.col("premium_tier_ref"), F.lit("#"), F.col("holder_id").cast("string")).alias("premium_tier_ref_ref"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "premium",
        "start_date",
        "end_date",
        "status",
        "premium_filled",
        "premium_filled_per_premium",
        F.round(F.col("premium_filled_per_premium") - F.avg("premium_filled_per_premium").over(w_start_date_month_status), 2).alias("premium_filled_per_premium_vs_start_date_month_s"),
        (F.col("premium_filled_per_premium") - F.lag("premium_filled_per_premium").over(w_holder_id_by_premium_filled)).alias("premium_filled_per_premium_change"),
        F.round(F.percent_rank().over(w_holder_id_by_premium_filled), 3).alias("premium_filled_per_premium_percent_rank"),
    )
)

result = (
    metrics
    .groupBy("end_date")
    .pivot("status", ["ACTIVE", "CANCELLED"])
    .agg(F.count("premium"))
    .orderBy("end_date")
)
