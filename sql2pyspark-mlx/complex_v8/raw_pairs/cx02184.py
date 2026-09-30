from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_end_date = Window.orderBy(F.col("end_date").desc(), "id")
w_holder_id_by_end_date_ties = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc())
w_all_by_end_date_full = Window.orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc(), "id")
w_dedup = Window.partitionBy("holder_id").orderBy("first_end_date", "id")

scoped = (
    df_policies
    .filter((F.col("start_date").isNotNull()) & (F.col("product") == "AUTO"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
    )
)

combined_details_holder = (
    scoped.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.segment").isin("RETAIL", "VIP"))
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.end_date_label",
        F.col("p.segment").alias("holder_segment"),
    )
)

windowed = (
    combined_details_holder
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "end_date_label",
        F.rank().over(w_all_by_end_date).alias("end_date_rank"),
        F.round(F.percent_rank().over(w_holder_id_by_end_date_ties), 3).alias("end_date_percent_rank"),
        F.first("end_date").over(w_all_by_end_date_full).alias("first_end_date"),
        F.row_number().over(w_all_by_end_date).alias("seq_no"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_holder_id_by_end_date)).alias("days_since_prev_end_date"),
    )
)

unique_rows = (
    windowed
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

derived = (
    unique_rows
    .select(
        "*",
        F.when(F.col("product").isin("TRAVEL", "AUTO"), "group_a").when(F.col("product") == "HOME", "group_b").otherwise("other").alias("product_group"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("first_end_date")).alias("days_since_first_end_date"),
    )
)

filtered = (
    derived.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.reported_at") < F.lit("2025-03-02 14:00:00").cast("timestamp")) & (F.col("c.claim_date") >= F.lit("2024-04-09").cast("date")), "left_anti")
    .filter(~F.col("product").isin("LIFE", "HOME"))
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.min("start_date").alias("first_policy_start_date"),
        F.round(F.avg("premium"), 2).alias("avg_policy_premium"),
    )
)

summarized_rows = (
    filtered.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.first_policy_start_date", "p.avg_policy_premium")
)

result = (
    summarized_rows
    .filter(F.col("days_since_first_end_date") < 525)
    .select(
        "id",
        "holder_id",
        "end_date",
        "end_date_rank",
        "seq_no",
        "days_since_prev_end_date",
        "product_group",
        "days_since_end_date",
        "days_since_first_end_date",
        "avg_policy_premium",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.coalesce(F.col("product_group"), F.lit("UNKNOWN")).alias("product_group_or_unknown"),
    )
    .orderBy(F.col("product_group").asc())
)
