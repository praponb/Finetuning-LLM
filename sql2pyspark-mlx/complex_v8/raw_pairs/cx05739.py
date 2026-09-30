from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_joined_on = Window.orderBy("joined_on", "id", "policy_id")
w_policy_id_policy_product = Window.partitionBy("policy_id", "policy_product")

prepared = (
    df_policyholders
    .filter(F.col("joined_on") >= F.lit("2024-03-22").cast("date"))
    .select(
        "id",
        "risk_score",
        "joined_on",
        "segment",
        F.lower(F.regexp_replace(F.trim(F.col("holder_name")), " ", "_")).alias("holder_name_slug"),
    )
)

exploded_detail_policys = (
    prepared.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.joined_on",
        F.col("p.id").alias("policy_id"),
        F.col("p.product").alias("policy_product"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

with_windows = (
    exploded_detail_policys
    .select(
        "id",
        "joined_on",
        "policy_id",
        "policy_product",
        "policy_premium",
        "policy_start_date",
        (F.col("policy_premium") - F.lag("policy_premium").over(w_all_by_joined_on)).alias("policy_premium_change"),
        F.round(F.percent_rank().over(w_all_by_joined_on), 3).alias("joined_on_percent_rank"),
        F.round(F.col("policy_premium") / F.nullif(F.sum("policy_premium").over(w_policy_id_policy_product), F.lit(0)), 4).alias("policy_premium_share_of_policy_id_policy_product"),
        F.max("policy_premium").over(w_policy_id_policy_product).alias("policy_id_policy_product_max_policy_premium"),
    )
)

without_matches = (
    with_windows.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.status").isNotNull()) & (F.col("p.start_date") < F.lit("2025-05-30").cast("date")), "left_anti")
    .filter(F.col("joined_on_percent_rank").between(0.1, 0.7))
)

result = (
    without_matches
    .groupBy("joined_on", "policy_start_date")
    .pivot("policy_product", ["HEALTH", "HOME"])
    .agg(F.count("policy_id_policy_product_max_policy_premium"))
)
