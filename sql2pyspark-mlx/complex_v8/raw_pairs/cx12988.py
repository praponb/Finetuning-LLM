from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_status_by_end_date = Window.partitionBy("holder_id", "status").orderBy("end_date_week", "id")
w_all_by_start_date_ties = Window.orderBy(F.col("start_date").desc())

scoped = (
    df_policies
    .filter(F.col("status").isin("ACTIVE", "EXPIRED", "CANCELLED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.round(F.col("premium") * 1.07, 2).alias("premium_adj"),
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
        F.date_trunc("week", F.col("end_date")).alias("end_date_week"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.min("end_date").alias("first_policy_end_date"))
)

profiled = (
    scoped.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.first_policy_end_date")
)

with_windows = (
    profiled
    .select(
        "*",
        F.datediff(F.col("first_policy_end_date"), F.lag("first_policy_end_date").over(w_holder_id_status_by_end_date)).alias("days_since_prev_first_policy_end_date"),
        F.lead("premium", 3, 0).over(w_holder_id_status_by_end_date).alias("next3_premium"),
        F.rank().over(w_all_by_start_date_ties).alias("start_date_rank"),
        F.round(F.percent_rank().over(w_holder_id_status_by_end_date), 3).alias("end_date_week_percent_rank"),
    )
)

with_details_holder = (
    with_windows.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.upper(F.trim(F.col("p.holder_name"))).like("G%")), "inner")
    .select("s.*", F.col("p.joined_on").alias("holder_joined_on"), "p.holder_name")
)

result = (
    with_details_holder
    .cube("product", "status", "start_date_rank")
    .agg(F.countDistinct("status").alias("distinct_status"))
    .orderBy(
        F.col("product").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("start_date_rank").asc_nulls_last(),
    )
)
