from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_avg_policy_premiu = Window.partitionBy("product").orderBy("avg_policy_premium", "id")
w_all_by_holder_joined_on_ties = Window.orderBy(F.col("holder_joined_on").desc())

filtered_policys = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-08-03").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.round(F.avg("premium"), 2).alias("avg_policy_premium"),
        F.max("start_date").alias("last_policy_start_date"),
    )
)

with_counts = (
    filtered_policys.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.avg_policy_premium", "p.last_policy_start_date")
)

enriched = (
    with_counts.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.avg_policy_premium",
        "s.last_policy_start_date",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment"),
    )
)

with_trends = (
    enriched
    .select(
        "id",
        "holder_id",
        "status",
        "avg_policy_premium",
        "last_policy_start_date",
        "holder_joined_on",
        "holder_segment",
        F.rank().over(w_product_by_avg_policy_premiu).alias("avg_policy_premium_rank"),
        F.round(F.percent_rank().over(w_all_by_holder_joined_on_ties), 3).alias("holder_joined_on_percent_rank"),
    )
)

result = (
    with_trends
    .groupBy("holder_id", "avg_policy_premium_rank")
    .pivot("status", ["EXPIRED", "CANCELLED", "ACTIVE"])
    .agg(F.max("avg_policy_premium"))
    .orderBy("holder_id", "avg_policy_premium_rank")
)
