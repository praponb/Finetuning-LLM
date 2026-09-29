from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_ties = Window.orderBy(F.col("start_date").desc())
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_all_by_start_date_running = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_end_date_month = Window.partitionBy("product", F.month("end_date"))

filtered_policys = (
    df_policies
    .filter((F.col("product").isNotNull()) & (F.col("holder_id") >= 10))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
        F.when(F.col("status").isin("CANCELLED", "EXPIRED"), "group_a").when(F.col("status") == "LAPSED", "group_b").otherwise("other").alias("status_group"),
        F.lower(F.trim(F.col("status"))).alias("status_clean_2"),
    )
)

with_activity = (
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.joined_on").isNotNull()) & (F.col("p.risk_score") < 0.8), "left_semi")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("reported_at").alias("first_claim_reported_at"),
        F.count(F.when(F.col("claim_type").isin("fire", "delay"), 1)).alias("claim_claim_type_count"),
    )
)

profiled = (
    with_activity.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.first_claim_reported_at",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

windowed = (
    profiled
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_start_date_ties), 3).alias("start_date_cume_dist"),
        F.row_number().over(w_all_by_start_date).alias("seq_no"),
        F.last("first_claim_reported_at", ignorenulls=True).over(w_all_by_start_date_running).alias("last_known_first_claim_reported_at"),
        F.count(F.lit(1)).over(w_product_end_date_month).alias("rows_in_product_end_date_month"),
    )
)

result = (
    windowed
    .groupingSets(
        [["status_group", "seq_no"], ["status_group"]],
        "status_group", "seq_no",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
    )
)
