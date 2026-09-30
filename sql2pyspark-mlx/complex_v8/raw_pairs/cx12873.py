from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_premium_tier_holder_segment = Window.partitionBy("premium_tier", "holder_segment")
w_status_holder_segment_by_hol = Window.partitionBy("status", "holder_segment").orderBy(F.col("holder_risk_score").desc(), "id")
w_status_holder_segment_by_hol_2 = Window.partitionBy("status", "holder_segment").orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_holder_segment_by_hol_3 = Window.partitionBy("status", "holder_segment").orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(-3, Window.currentRow)

base = (
    df_policies
    .filter((F.col("start_date").between(F.lit("2024-10-22").cast("date"), F.lit("2025-08-24").cast("date"))) & (F.col("holder_id") <= 4))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.date_add(F.col("end_date"), 30).alias("end_date_plus_30d"),
    )
)

tagged = (
    base
    .filter(F.col("status").isNotNull())
    .select(
        "*",
        F.when(F.col("premium") >= 3544.1, "large").when(F.col("premium") >= 2111.3, "medium").otherwise("small").alias("premium_tier"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

joined = (
    tagged.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

ranked = (
    joined
    .select(
        "*",
        F.size(F.collect_set("holder_id").over(w_premium_tier_holder_segment)).alias("distinct_holder_id_in_premium_tier_holder_segmen"),
        F.rank().over(w_status_holder_segment_by_hol).alias("holder_risk_score_rank"),
        F.first("holder_risk_score").over(w_status_holder_segment_by_hol_2).alias("first_holder_risk_score"),
        F.sum("product_len").over(w_status_holder_segment_by_hol_3).alias("product_len_sum_4"),
        F.sum("holder_risk_score").over(w_premium_tier_holder_segment).alias("premium_tier_holder_segment_sum_holder_risk_scor"),
    )
)

result = (
    ranked
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        "status_slug",
        "end_date_plus_30d",
        "premium_tier",
        "holder_region",
        "holder_risk_score_rank",
        "product_len_sum_4",
        "premium_tier_holder_segment_sum_holder_risk_scor",
        F.abs(F.col("first_holder_risk_score") - 0.6).alias("first_holder_risk_score_distance"),
    )
    .orderBy(F.col("first_holder_risk_score_distance").asc_nulls_last(), "id")
    .limit(10)
)
