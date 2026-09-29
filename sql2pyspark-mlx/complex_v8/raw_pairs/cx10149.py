from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_holder_segment = Window.partitionBy("holder_id", "holder_segment")
w_holder_segment_by_end_date_q = Window.partitionBy("holder_segment").orderBy(F.col("end_date_quarter").desc(), "id")
w_holder_id_by_end_date_runnin = Window.partitionBy("holder_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_policies
    .filter((F.col("end_date") >= F.lit("2025-12-05").cast("date")) | (F.col("start_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
        F.date_trunc("quarter", F.col("end_date")).alias("end_date_quarter"),
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
    )
)

screened = (
    valid
    .crossJoin(valid.agg(F.avg("premium").alias("overall_avg_premium")))
    .filter(F.col("premium") > F.col("overall_avg_premium"))
    .drop("overall_avg_premium")
)

combined_details_holder = (
    screened.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.lower(F.col("p.holder_name")).like("%gu%")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        "s.status_prefix",
        "s.end_date_quarter",
        "s.product_prefix",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment"),
    )
)

screened_2 = (
    combined_details_holder.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.hour(F.col("c.reported_at")).between(10, 13)), "left_semi")
)

metrics = (
    screened_2
    .select(
        "*",
        F.round(F.avg("premium").over(w_holder_id_holder_segment), 2).alias("holder_id_holder_segment_avg_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_holder_segment), 2).alias("premium_vs_holder_id_holder_segment_avg"),
        F.count(F.lit(1)).over(w_holder_id_holder_segment).alias("rows_in_holder_id_holder_segment"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_holder_segment), F.lit(0)), 4).alias("premium_share_of_holder_id_holder_segment"),
        F.round((F.unix_timestamp("end_date_quarter") - F.unix_timestamp(F.lag("end_date_quarter").over(w_holder_segment_by_end_date_q))) / 60, 1).alias("minutes_since_prev_end_date_quarter"),
        F.last("premium", ignorenulls=True).over(w_holder_id_by_end_date_runnin).alias("last_known_premium"),
    )
)

screened_3 = (
    metrics.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_date") < F.lit("2025-09-14").cast("date")), "left_semi")
    .filter(F.col("status_prefix").isNotNull())
)

holder_segment_stats = (
    screened_3
    .groupBy("holder_segment")
    .agg(F.avg("minutes_since_prev_end_date_quarter").alias("avg_minutes_since_prev_end_date_quarter"))
)

vs_group = (
    screened_3.alias("s")
    .join(holder_segment_stats.alias("g"), F.col("g.holder_segment") == F.col("s.holder_segment"), "inner")
    .filter(F.col("s.minutes_since_prev_end_date_quarter") > F.col("g.avg_minutes_since_prev_end_date_quarter"))
    .select(
        "s.*",
        "g.avg_minutes_since_prev_end_date_quarter",
        F.round(F.col("s.minutes_since_prev_end_date_quarter") - F.col("g.avg_minutes_since_prev_end_date_quarter"), 2).alias("minutes_since_prev_end_date_quarter_minus_avg"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "premium_vs_holder_id_holder_segment_avg",
        "holder_id_holder_segment_avg_premium",
        "avg_minutes_since_prev_end_date_quarter",
    )
    .unpivot(
        ids=["id"],
        values=["premium_vs_holder_id_holder_segment_avg", "holder_id_holder_segment_avg_premium", "avg_minutes_since_prev_end_date_quarter"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
