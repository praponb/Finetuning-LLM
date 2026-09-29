from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_month_status_group = Window.partitionBy(F.date_trunc("month", "end_date"), "status_group").orderBy("premium", "id")
w_end_date_month_status_group_2 = Window.partitionBy(F.date_trunc("month", "end_date"), "status_group").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_latest = Window.partitionBy("status_group").orderBy(F.col("seq_no").desc(), F.col("id").desc())

filtered_policys = (
    df_policies
    .filter((F.col("status") != "ACTIVE") & (F.col("product") == "HEALTH"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
        F.when(F.col("status").isin("ACTIVE", "LAPSED"), "group_a").when(F.col("status") == "EXPIRED", "group_b").otherwise("other").alias("status_group"),
    )
)

with_trends = (
    filtered_policys
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "product_prefix",
        "status_group",
        F.row_number().over(w_end_date_month_status_group).alias("seq_no"),
        F.ntile(4).over(w_end_date_month_status_group).alias("premium_quartile"),
        F.lag("premium").over(w_end_date_month_status_group).alias("prev_premium"),
        F.nth_value("end_date", 2).over(w_end_date_month_status_group_2).alias("second_end_date"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
        F.max("claim_date").alias("last_claim_date"),
        F.count("*").alias("claim_count"),
    )
)

with_stats = (
    with_trends.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.min_claim_claimed_amount",
        "c.last_claim_date",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

combined_details_holder = (
    with_stats.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.product_prefix",
        "s.status_group",
        "s.seq_no",
        "s.premium_quartile",
        "s.second_end_date",
        "s.min_claim_claimed_amount",
        "s.last_claim_date",
        F.col("p.region").alias("holder_region"),
        "p.holder_name",
    )
)

filtered = (
    combined_details_holder.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status") == "LAPSED") & (F.year(F.col("p.end_date")) == 2025), "left_anti")
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .select(
        "id",
        "holder_id",
        "premium",
        "product_prefix",
        "status_group",
        "premium_quartile",
        "second_end_date",
        "min_claim_claimed_amount",
        "last_claim_date",
        "holder_region",
        "holder_name",
        F.lower(F.regexp_replace(F.trim(F.col("holder_name")), " ", "_")).alias("holder_name_slug"),
    )
    .orderBy(F.col("holder_id").asc_nulls_last())
)
