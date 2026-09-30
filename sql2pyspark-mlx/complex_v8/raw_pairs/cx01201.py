from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_year_holder_id = Window.partitionBy(F.year("claim_date"), "holder_id")
w_claim_id_by_claim_approved_a = Window.partitionBy("claim_id").orderBy(F.col("claim_approved_amount").desc(), "id", "claim_id")
w_claim_id_by_claim_approved_a_2 = Window.partitionBy("claim_id").orderBy(F.col("claim_approved_amount").desc(), "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_latest = Window.partitionBy("holder_id").orderBy("start_date", "id", "claim_id")

filtered_policys = (
    df_policies
    .filter((F.col("holder_id") >= 8) & (F.col("premium") <= 4678.8))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.quarter(F.col("start_date")).alias("start_date_quarter"),
        F.quarter(F.col("end_date")).alias("end_date_quarter"),
        F.date_format(F.col("end_date"), "MMM yyyy").alias("end_date_label"),
    )
)

line_level = (
    filtered_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.start_date_quarter",
        "s.end_date_quarter",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        "c.claim_date",
    )
)

ranked = (
    line_level
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        "start_date_quarter",
        "end_date_quarter",
        "claim_id",
        "claim_approved_amount",
        "claim_date",
        F.round(F.col("start_date_quarter") / F.nullif(F.sum("start_date_quarter").over(w_claim_date_year_holder_id), F.lit(0)), 4).alias("start_date_quarter_share_of_claim_date_year_hold"),
        F.dense_rank().over(w_claim_id_by_claim_approved_a).alias("claim_approved_amount_dense_rank"),
        F.nth_value("premium", 2).over(w_claim_id_by_claim_approved_a_2).alias("second_premium"),
        F.round(F.col("claim_approved_amount") - F.avg("claim_approved_amount").over(w_claim_date_year_holder_id), 2).alias("claim_approved_amount_vs_claim_date_year_holder_"),
        F.lag("claim_approved_amount").over(w_claim_id_by_claim_approved_a).alias("prev_claim_approved_amount"),
    )
)

status_stats = (
    ranked
    .groupBy("status")
    .agg(F.avg("start_date_quarter").alias("avg_start_date_quarter"))
)

vs_group = (
    ranked.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.start_date_quarter") > F.col("g.avg_start_date_quarter"))
    .select(
        "s.*",
        "g.avg_start_date_quarter",
        F.round(F.col("s.start_date_quarter") - F.col("g.avg_start_date_quarter"), 2).alias("start_date_quarter_minus_avg"),
    )
)

latest = (
    vs_group
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

annotated = (
    latest
    .filter(F.col("status") == "CANCELLED")
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        "start_date_quarter",
        "end_date_quarter",
        "claim_id",
        "start_date_quarter_share_of_claim_date_year_hold",
        "claim_approved_amount_dense_rank",
        "second_premium",
        "prev_claim_approved_amount",
        "avg_start_date_quarter",
        "start_date_quarter_minus_avg",
        F.date_trunc("year", F.col("start_date")).alias("start_date_year"),
        F.when(F.col("start_date_quarter") >= 3, "HIGH").when(F.col("start_date_quarter") >= 2, "MEDIUM").otherwise("LOW").alias("start_date_quarter_tier"),
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
    )
)

result = (
    annotated
    .select(
        "id",
        "claim_id",
        F.col("second_premium").alias("m_second_premium"),
        F.col("start_date_quarter_minus_avg").alias("m_start_date_quarter_minus_avg"),
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["m_second_premium", "m_start_date_quarter_minus_avg"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "claim_id", "metric_name")
)
