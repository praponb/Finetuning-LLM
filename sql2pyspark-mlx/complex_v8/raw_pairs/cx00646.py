from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_premium_ties = Window.partitionBy("holder_id").orderBy("premium")
w_all_by_end_date_full = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_policys = df_policies

metrics = (
    filtered_policys
    .withColumn("premium_percent_rank", F.round(F.percent_rank().over(w_holder_id_by_premium_ties), 3))
    .withColumn("second_start_date", F.nth_value("start_date", 2).over(w_all_by_end_date_full))
)

holder_id_stats = (
    metrics
    .groupBy("holder_id")
    .agg(F.max("premium").alias("max_premium"))
)

above_average = (
    metrics.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium") == F.col("g.max_premium"))
    .select(
        "s.*",
        "g.max_premium",
        F.round(F.col("s.premium") - F.col("g.max_premium"), 2).alias("premium_minus_max"),
    )
)

detail_rows = (
    above_average.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.premium_percent_rank",
        "s.max_premium",
        "s.premium_minus_max",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

result = (
    detail_rows
    .rollup("holder_id", "claim_adjuster_id")
    .agg(
        F.max("premium_minus_max").alias("max_premium_minus_max"),
        F.min("start_date").alias("first_start_date"),
    )
)
