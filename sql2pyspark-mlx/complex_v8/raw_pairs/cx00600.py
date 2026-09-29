from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_joined_on_month = Window.partitionBy(F.date_trunc("month", "holder_joined_on"))
w_all_by_holder_joined_on_prio = Window.orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(-2, -1)
w_all_by_holder_joined_on_movi = Window.orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_holder_joined_on = Window.orderBy("holder_joined_on", "id")

prepared = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2025-04-17").cast("date"), F.lit("2026-09-19").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.when(F.col("product").isin("TRAVEL", "LIFE"), "group_a").when(F.col("product") == "HOME", "group_b").otherwise("other").alias("product_group"),
    )
)

enriched_holder = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score") <= 0.8), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.product_group",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment"),
    )
)

ranked = (
    enriched_holder
    .select(
        "*",
        F.round(F.col("premium") - F.avg("premium").over(w_holder_joined_on_month), 2).alias("premium_vs_holder_joined_on_month_avg"),
        F.max("holder_risk_score").over(w_all_by_holder_joined_on_prio).alias("holder_risk_score_max_2_prior"),
        F.round(F.avg("premium").over(w_all_by_holder_joined_on_movi), 2).alias("premium_avg_4"),
        F.row_number().over(w_all_by_holder_joined_on).alias("rn"),
        F.round(F.col("holder_risk_score") - F.avg("holder_risk_score").over(w_holder_joined_on_month), 2).alias("holder_risk_score_vs_holder_joined_on_month_avg"),
    )
)

enriched = (
    ranked
    .filter(F.col("holder_segment") == "VIP")
    .withColumn("holder_segment_prefix", F.substring(F.trim(F.col("holder_segment")), 1, 3))
)

joined_holder = (
    enriched.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.holder_risk_score",
        "s.holder_risk_score_max_2_prior",
        "s.premium_avg_4",
        "s.rn",
        "s.holder_segment_prefix",
        "p.holder_name",
    )
)

without_matches = (
    joined_holder.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") < 3465.4) & (F.year(F.col("p.start_date")) == 2025), "left_anti")
)

result = (
    without_matches
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "holder_risk_score",
        "holder_risk_score_max_2_prior",
        "premium_avg_4",
        "rn",
        "holder_segment_prefix",
        "holder_name",
        F.concat_ws("|", F.col("holder_name"), F.col("holder_segment_prefix")).alias("holder_name_holder_segment_prefix_label"),
        F.lower(F.regexp_replace(F.trim(F.col("holder_name")), " ", "_")).alias("holder_name_slug"),
    )
    .orderBy(F.col("holder_risk_score_max_2_prior").desc(), "id")
    .limit(5)
)
