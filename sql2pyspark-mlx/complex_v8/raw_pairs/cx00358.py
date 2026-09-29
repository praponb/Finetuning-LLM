from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_holder_risk_score_ful = Window.orderBy("holder_risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_segment = Window.partitionBy("holder_segment")
w_all_by_holder_risk_score_run = Window.orderBy("holder_risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_policies
    .filter((F.col("product") == "HOME") & (F.col("end_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

combined_details_holder = (
    valid.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.product_slug",
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

holder_segment_stats = (
    combined_details_holder
    .groupBy("holder_segment")
    .agg(F.avg("premium").alias("avg_premium"))
)

outliers = (
    combined_details_holder.alias("s")
    .join(holder_segment_stats.alias("g"), F.col("g.holder_segment") == F.col("s.holder_segment"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

with_trends = (
    outliers
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "product_slug",
        "holder_segment",
        "holder_risk_score",
        "avg_premium",
        "premium_minus_avg",
        F.nth_value("avg_premium", 2).over(w_all_by_holder_risk_score_ful).alias("second_avg_premium"),
        F.size(F.collect_set("holder_id").over(w_holder_segment)).alias("distinct_holder_id_in_holder_segment"),
        F.count(F.lit(1)).over(w_holder_segment).alias("rows_in_holder_segment"),
        F.max("premium_minus_avg").over(w_all_by_holder_risk_score_run).alias("max_premium_minus_avg_so_far"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "start_date",
        "product_slug",
        "holder_segment",
        "holder_risk_score",
        "avg_premium",
        "second_avg_premium",
        "distinct_holder_id_in_holder_segment",
        "rows_in_holder_segment",
        "max_premium_minus_avg_so_far",
        F.when(F.col("holder_segment").isin("RETAIL", "VIP"), "group_a").when(F.col("holder_segment") == "SME", "group_b").otherwise("other").alias("holder_segment_group"),
    )
    .orderBy(F.col("holder_risk_score").desc(), "id")
    .limit(5)
)
