from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id").orderBy(F.col("start_date").desc(), F.col("id").desc())
w_policy_count_by_policy_produ = Window.partitionBy("policy_count").orderBy("policy_product_count", "id")
w_policy_status_count = Window.partitionBy("policy_status_count")

recent_policys = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("status").isin("EXPIRED", "ACTIVE")))
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("policy_status_count"),
        F.count("*").alias("policy_count"),
        F.count(F.when(F.col("product") == "TRAVEL", 1)).alias("policy_product_count"),
    )
)

profiled = (
    recent_policys.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_status_count"), F.lit(0)).alias("policy_status_count"),
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        F.coalesce(F.col("p.policy_product_count"), F.lit(0)).alias("policy_product_count"),
    )
)

deduped = (
    profiled
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_trends = (
    deduped
    .select(
        "*",
        F.round(F.cume_dist().over(w_policy_count_by_policy_produ), 3).alias("policy_product_count_cume_dist"),
        F.sum("policy_product_count").over(w_policy_status_count).alias("policy_status_count_sum_policy_product_count"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_policy_status_count), F.lit(0)), 4).alias("premium_share_of_policy_status_count"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "policy_product_count",
        "policy_product_count_cume_dist",
        "policy_status_count_sum_policy_product_count",
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
    .orderBy("holder_id", "id")
    .limit(20)
)
