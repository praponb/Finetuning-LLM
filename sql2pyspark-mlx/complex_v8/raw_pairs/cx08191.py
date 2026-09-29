from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_policy_count_by_premi = Window.partitionBy("status", "policy_count").orderBy(F.col("premium").desc(), "id")
w_status_policy_count_by_premi_2 = Window.partitionBy("status", "policy_count").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_month_holder_id_b = Window.partitionBy(F.date_trunc("month", "start_date"), "holder_id").orderBy("policy_count_2", "id")
w_product = Window.partitionBy("product")
w_holder_id_status_by_policy_c = Window.partitionBy("holder_id", "status").orderBy(F.col("policy_count").desc(), "id")
w_status_policy_count_by_premi_3 = Window.partitionBy("status", "policy_count").orderBy(F.col("premium").desc(), "id").rowsBetween(-3, -1)

valid = (
    df_policies
    .filter((F.col("premium") <= 4670.9) & (F.col("start_date") < F.lit("2025-03-21").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.initcap(F.trim(F.col("product"))).alias("product_clean"),
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
        F.date_format(F.col("start_date"), "yyyy-MM-dd").alias("start_date_label"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.count("*").alias("policy_count_2"),
    )
)

with_counts = (
    valid.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        F.coalesce(F.col("p.policy_count_2"), F.lit(0)).alias("policy_count_2"),
    )
)

without_matches = (
    with_counts.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.start_date") < F.lit("2025-02-14").cast("date")) & (F.col("p.status").isin("EXPIRED", "ACTIVE")), "left_anti")
    .filter(F.col("start_date_label").isNotNull())
)

joined_holder = (
    without_matches.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score") < 0.8), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.product_clean",
        "s.policy_count",
        "s.policy_count_2",
        F.col("p.region").alias("holder_region"),
        "p.holder_name",
    )
)

sequenced = (
    joined_holder
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        "product_clean",
        "policy_count",
        "holder_region",
        "holder_name",
        F.lead("start_date", 3).over(w_status_policy_count_by_premi).alias("next3_start_date"),
        F.nth_value("start_date", 2).over(w_status_policy_count_by_premi_2).alias("second_start_date"),
        F.row_number().over(w_start_date_month_holder_id_b).alias("rn"),
        F.round(F.col("policy_count") / F.nullif(F.sum("policy_count").over(w_product), F.lit(0)), 4).alias("policy_count_share_of_product"),
        F.lag("policy_count_2").over(w_holder_id_status_by_policy_c).alias("prev_policy_count_2"),
        F.max("premium").over(w_status_policy_count_by_premi_3).alias("premium_max_3_prior"),
    )
)

result = (
    sequenced
    .filter(F.lower(F.col("holder_name")).like("%no%"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "product_clean",
        "policy_count",
        "holder_region",
        "holder_name",
        "rn",
        "policy_count_share_of_product",
        "prev_policy_count_2",
        "premium_max_3_prior",
        F.concat(F.col("holder_name"), F.lit("#"), F.col("id").cast("string")).alias("holder_name_ref"),
    )
    .orderBy(
        F.col("start_date").asc_nulls_last(),
        F.col("prev_policy_count_2").asc_nulls_last(),
    )
)
