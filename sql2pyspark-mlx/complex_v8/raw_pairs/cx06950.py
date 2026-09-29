from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")

recent_policys = (
    df_policies
    .filter(F.col("end_date") >= F.lit("2026-02-16").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

without_matches = (
    recent_policys.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status") == "CANCELLED") & (F.col("p.product") != "TRAVEL"), "left_semi")
)

totals = (
    without_matches
    .filter(F.col("premium") > 2111.3)
    .groupBy("holder_id", "product")
    .agg(
        F.sum(F.when(F.col("product") == "HEALTH", F.col("premium"))).alias("premium_product_health"),
        F.sum(F.when(~F.col("product").isin("HOME", "HEALTH"), F.col("premium"))).alias("premium_product_not_in_home_health"),
        F.round(F.sum("premium"), 2).alias("total_premium"),
        F.min("start_date").alias("first_start_date"),
        F.count("*").alias("cnt"),
    )
)

exploded_detail = (
    totals.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "inner")
    .select(
        "s.holder_id",
        "s.product",
        "s.premium_product_not_in_home_health",
        "s.total_premium",
        "s.first_start_date",
        "s.cnt",
        F.col("p.id").alias("policy_id"),
        F.col("p.product").alias("policy_product"),
        F.col("p.premium").alias("policy_premium"),
    )
)

with_windows = (
    exploded_detail
    .select(
        "*",
        F.round(F.col("cnt") / F.nullif(F.sum("cnt").over(w_product), F.lit(0)), 4).alias("cnt_share_of_product"),
        F.size(F.collect_set("policy_product").over(w_product)).alias("distinct_policy_product_in_product"),
        F.round(F.col("total_premium") / F.nullif(F.sum("total_premium").over(w_product), F.lit(0)), 4).alias("total_premium_share_of_product"),
        F.size(F.collect_set("holder_id").over(w_product)).alias("distinct_holder_id_in_product"),
    )
)

with_activity = (
    with_windows.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.start_date") < F.lit("2025-03-24").cast("date")), "left_anti")
)

result = (
    with_activity
    .filter(F.col("total_premium") < 19570.0)
    .select(
        "holder_id",
        "premium_product_not_in_home_health",
        "total_premium",
        "first_start_date",
        "cnt",
        "policy_id",
        "policy_product",
        "policy_premium",
        "cnt_share_of_product",
        "distinct_policy_product_in_product",
        "total_premium_share_of_product",
        "distinct_holder_id_in_product",
        F.concat_ws("-", F.col("product"), F.col("policy_product")).alias("product_policy_product_label"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
    .orderBy("cnt", F.col("policy_id").desc())
)
