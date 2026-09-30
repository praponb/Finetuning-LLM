from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region_group_by_last_policy = Window.partitionBy("region_group").orderBy(F.col("last_policy_start_date_plus_3m").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_region_group_by_last_policy_2 = Window.partitionBy("region_group").orderBy(F.col("last_policy_start_date_plus_3m").desc(), "id")
w_region_group_by_last_policy_3 = Window.partitionBy("region_group").orderBy(F.col("last_policy_start_date").desc(), "id")

base = (
    df_policyholders
    .select(
        "id",
        "region",
        "joined_on",
        "segment",
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.max("start_date").alias("last_policy_start_date"))
)

with_counts = (
    base.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select("s.*", "p.last_policy_start_date")
)

derived = (
    with_counts
    .select(
        "id",
        "region",
        "segment",
        "last_policy_start_date",
        F.add_months(F.col("last_policy_start_date"), 3).alias("last_policy_start_date_plus_3m"),
        F.when(F.col("region").isin("South", "East"), "group_a").when(F.col("region") == "North", "group_b").otherwise("other").alias("region_group"),
    )
)

filtered = (
    derived.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.product") == "HOME") & (~F.col("p.status").isin("CANCELLED", "ACTIVE")), "left_anti")
    .filter(~F.col("segment").isin("VIP", "SME"))
)

with_trends = (
    filtered
    .select(
        "id",
        "region",
        "segment",
        "last_policy_start_date",
        "last_policy_start_date_plus_3m",
        "region_group",
        F.nth_value("last_policy_start_date", 2).over(w_region_group_by_last_policy).alias("second_last_policy_start_date"),
        F.lead("last_policy_start_date", 3).over(w_region_group_by_last_policy_2).alias("next3_last_policy_start_date"),
        F.lag("last_policy_start_date").over(w_region_group_by_last_policy_3).alias("prev_last_policy_start_date"),
        F.lead("last_policy_start_date", 3).over(w_region_group_by_last_policy_3).alias("next3_last_policy_start_date_2"),
        F.row_number().over(w_region_group_by_last_policy_3).alias("rn"),
        F.lag("last_policy_start_date").over(w_region_group_by_last_policy_2).alias("prev_last_policy_start_date_2"),
    )
)

line_level_policys = (
    with_trends.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.region",
        "s.segment",
        "s.last_policy_start_date",
        "s.last_policy_start_date_plus_3m",
        "s.second_last_policy_start_date",
        "s.next3_last_policy_start_date",
        "s.prev_last_policy_start_date",
        "s.next3_last_policy_start_date_2",
        "s.rn",
        "s.prev_last_policy_start_date_2",
        F.col("p.id").alias("policy_id"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.product").alias("policy_product"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    line_level_policys
    .groupBy("policy_end_date", "next3_last_policy_start_date")
    .pivot("segment", ["RETAIL", "VIP", "SME"])
    .agg(F.count("rn"))
    .orderBy("policy_end_date", "next3_last_policy_start_date")
)
