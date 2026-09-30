from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment_region_by_joined_on = Window.partitionBy("segment", "region").orderBy(F.col("joined_on").desc(), "id")
w_segment_region_by_joined_on_2 = Window.partitionBy("segment", "region").orderBy(F.col("joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_holders = (
    df_policyholders
    .filter((F.col("region") != "South") | (F.upper(F.trim(F.col("holder_name"))).like("L%")))
    .select(
        "id",
        "region",
        "joined_on",
        "segment",
        F.concat_ws("-", F.col("segment"), F.col("holder_name")).alias("segment_holder_name_label"),
        F.lower(F.regexp_replace(F.trim(F.col("holder_name")), " ", "_")).alias("holder_name_slug"),
    )
)

ranked = (
    filtered_holders
    .select(
        "*",
        F.lag("joined_on").over(w_segment_region_by_joined_on).alias("prev_joined_on"),
        F.rank().over(w_segment_region_by_joined_on).alias("joined_on_rank"),
        F.first("joined_on").over(w_segment_region_by_joined_on_2).alias("first_joined_on"),
        F.datediff(F.col("joined_on"), F.lag("joined_on").over(w_segment_region_by_joined_on)).alias("days_since_prev_joined_on"),
    )
)

labeled = (
    ranked
    .filter(F.col("joined_on_rank").between(2, 7))
    .select(
        "*",
        F.upper(F.trim(F.col("holder_name_slug"))).alias("holder_name_slug_clean"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("joined_on")).alias("days_since_joined_on"),
        F.concat(F.col("segment"), F.lit("#"), F.col("joined_on_rank").cast("string")).alias("segment_ref"),
    )
)

exploded_detail_policys = (
    labeled.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_name_slug",
        "s.days_since_joined_on",
        "s.segment_ref",
        F.col("p.id").alias("policy_id"),
        F.col("p.product").alias("policy_product"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
    )
)

tagged = (
    exploded_detail_policys
    .select(
        "*",
        (F.col("policy_premium") + F.col("days_since_joined_on")).alias("policy_premium_plus_days_since_joined_on"),
    )
)

joined = (
    tagged.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.holder_id").isNotNull())
    .select("s.*", F.col("p.holder_id").alias("policy_holder_id"))
)

enriched_holder = (
    joined.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.policy_holder_id"), "inner")
    .join(df_policies.alias("po"), F.col("po.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.holder_name_slug",
        "s.days_since_joined_on",
        "s.segment_ref",
        "s.policy_id",
        "s.policy_product",
        "s.policy_start_date",
        "s.policy_premium",
        "s.policy_status",
        "s.policy_holder_id",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("po.status").alias("policy_status_2"),
    )
)

result = (
    enriched_holder
    .select(
        "id",
        "holder_name_slug",
        "days_since_joined_on",
        "segment_ref",
        "policy_id",
        "policy_product",
        "policy_start_date",
        "policy_status",
        "policy_holder_id",
        "holder_risk_score",
        "holder_joined_on",
        "policy_status_2",
        F.length(F.trim(F.col("holder_name_slug"))).alias("holder_name_slug_len"),
        F.substring(F.trim(F.col("policy_status")), 1, 3).alias("policy_status_prefix"),
    )
    .orderBy(F.col("holder_name_slug_len").asc(), "id", "policy_id")
    .limit(5)
)
