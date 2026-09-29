from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_holder_segments_hol = Window.partitionBy("distinct_holder_segments", F.to_date("holder_joined_on_2_month")).orderBy("n_product_in_travel_auto_healt", "holder_joined_on_2_month")
w_distinct_holder_segments_hol_2 = Window.partitionBy("distinct_holder_segments", F.to_date("holder_joined_on_2_month")).orderBy("n_product_in_travel_auto_healt")
w_distinct_holder_segments = Window.partitionBy("distinct_holder_segments")

prepared = (
    df_policies
    .filter(F.col("holder_id") >= 13)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

joined_holder = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

enriched_holder = (
    joined_holder.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.segment").alias("holder_segment_2"),
        F.col("p.risk_score").alias("holder_risk_score_2"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

combined_details_holder = (
    enriched_holder.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.joined_on") < F.lit("2025-07-03").cast("date")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.holder_segment",
        "s.holder_segment_2",
        "s.holder_risk_score_2",
        "s.holder_joined_on",
        F.col("p.joined_on").alias("holder_joined_on_2"),
        F.col("p.region").alias("holder_region"),
    )
)

stats = (
    combined_details_holder
    .groupBy(
        F.date_trunc("month", "holder_joined_on_2").alias("holder_joined_on_2_month"),
    )
    .agg(
        F.countDistinct("holder_segment").alias("distinct_holder_segments"),
        F.round(F.sum("holder_risk_score_2"), 2).alias("total_holder_risk_score_2"),
        F.sum(F.when(~F.col("holder_region").isin("West", "North"), 1).otherwise(0)).alias("n_holder_region_not_in_west_no"),
        F.count(F.when(F.col("product").isin("TRAVEL", "AUTO", "HEALTH"), 1)).alias("n_product_in_travel_auto_healt"),
        F.round(F.avg("holder_risk_score_2"), 2).alias("avg_holder_risk_score_2"),
        F.sum(F.when(F.col("holder_segment") == "VIP", F.col("holder_risk_score_2"))).alias("holder_risk_score_2_holder_segment_vip"),
    )
)

with_trends = (
    stats
    .select(
        "*",
        F.round(F.cume_dist().over(w_distinct_holder_segments_hol), 3).alias("n_product_in_travel_auto_healt_cume_dist"),
        F.round(F.cume_dist().over(w_distinct_holder_segments_hol_2), 3).alias("n_product_in_travel_auto_healt_cume_dist_2"),
        F.max("total_holder_risk_score_2").over(w_distinct_holder_segments).alias("distinct_holder_segments_max_total_holder_risk_s"),
    )
)

screened = (
    with_trends
    .crossJoin(with_trends.agg(F.max("distinct_holder_segments").alias("overall_max_distinct_holder_segments")))
    .filter(F.col("distinct_holder_segments") < F.col("overall_max_distinct_holder_segments"))
    .drop("overall_max_distinct_holder_segments")
)

result = (
    screened
    .select(
        "holder_joined_on_2_month",
        "total_holder_risk_score_2",
        "n_holder_region_not_in_west_no",
        "n_product_in_travel_auto_healt",
        "n_product_in_travel_auto_healt_cume_dist",
        "n_product_in_travel_auto_healt_cume_dist_2",
        "distinct_holder_segments_max_total_holder_risk_s",
        F.coalesce(F.col("avg_holder_risk_score_2"), F.lit(0.0)).alias("avg_holder_risk_score_2_filled"),
    )
)
