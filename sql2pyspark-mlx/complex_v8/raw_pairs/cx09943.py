from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("holder_segment", "holder_segment_group").orderBy(F.col("premium").desc(), F.col("id").desc())
w_latest = Window.partitionBy("holder_segment_group").orderBy(F.col("holder_joined_on").desc(), F.col("id").desc())

filtered = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

with_details_holder = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.joined_on") < F.lit("2024-12-21").cast("date")), "inner")
    .select("s.*", F.col("p.segment").alias("holder_segment"))
)

status_stats = (
    with_details_holder
    .groupBy("status")
    .agg(F.percentile_approx("premium", 0.5).alias("median_premium"))
)

above_average = (
    with_details_holder.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.premium") >= F.col("g.median_premium"))
    .select(
        "s.*",
        "g.median_premium",
        F.round(F.col("s.premium") - F.col("g.median_premium"), 2).alias("premium_minus_median"),
    )
)

enriched = (
    above_average
    .filter(F.col("status").isin("ACTIVE", "LAPSED"))
    .select(
        "*",
        F.round(F.col("median_premium") / F.nullif(F.col("premium_minus_median"), F.lit(0)), 3).alias("median_premium_per_premium_minus_median"),
        F.when(F.col("holder_segment").isin("RETAIL", "VIP"), "group_a").when(F.col("holder_segment") == "SME", "group_b").otherwise("other").alias("holder_segment_group"),
    )
)

joined_holder = (
    enriched.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.holder_name").isNotNull()), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.status",
        "s.premium_filled",
        "s.holder_segment",
        "s.median_premium",
        "s.premium_minus_median",
        "s.median_premium_per_premium_minus_median",
        "s.holder_segment_group",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

unique_rows = (
    joined_holder
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

deduped = (
    unique_rows
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        "premium_filled",
        "holder_segment",
        "median_premium",
        "premium_minus_median",
        "median_premium_per_premium_minus_median",
        "holder_segment_group",
        "holder_joined_on",
        "holder_risk_score",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.concat_ws(": ", F.col("product"), F.col("status")).alias("product_status_label"),
    )
    .orderBy("id", F.col("holder_segment_group").desc())
)
