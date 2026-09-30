from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("holder_id", "status").orderBy(F.col("start_date").desc(), F.col("id").desc())
w_status = Window.partitionBy("status")
w_all_by_end_date_prior_6 = Window.orderBy("end_date", "id").rowsBetween(-6, -1)
w_status_max_policy_premium_ti = Window.partitionBy("status", "max_policy_premium_tier").orderBy(F.col("premium_distance_minus_avg").desc(), "id")

recent = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) | (F.col("status") == "ACTIVE"))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.abs(F.col("premium") - 2844.1).alias("premium_distance"),
    )
)

deduped = (
    recent
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.min("end_date").alias("first_policy_end_date"),
        F.max("premium").alias("max_policy_premium"),
    )
)

with_counts = (
    deduped.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.first_policy_end_date", "p.max_policy_premium")
)

labeled = (
    with_counts
    .select(
        "*",
        F.abs(F.col("max_policy_premium") - 3560.0).alias("max_policy_premium_distance"),
        F.when(F.col("max_policy_premium") >= 4280.0, "HIGH").when(F.col("max_policy_premium") >= 1400.0, "MEDIUM").otherwise("LOW").alias("max_policy_premium_tier"),
        (F.col("max_policy_premium") - F.col("premium_distance")).alias("max_policy_premium_minus_premium_distance"),
    )
)

holder_id_stats = (
    labeled
    .groupBy("holder_id")
    .agg(F.avg("premium_distance").alias("avg_premium_distance"))
)

vs_group = (
    labeled.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium_distance") > F.col("g.avg_premium_distance"))
    .select(
        "s.*",
        "g.avg_premium_distance",
        F.round(F.col("s.premium_distance") - F.col("g.avg_premium_distance"), 2).alias("premium_distance_minus_avg"),
    )
)

ranked = (
    vs_group
    .select(
        "*",
        F.round(F.col("max_policy_premium_distance") / F.nullif(F.sum("max_policy_premium_distance").over(w_status), F.lit(0)), 4).alias("max_policy_premium_distance_share_of_status"),
        F.sum("avg_premium_distance").over(w_all_by_end_date_prior_6).alias("avg_premium_distance_sum_6_prior"),
        (F.col("max_policy_premium") - F.lag("max_policy_premium").over(w_status_max_policy_premium_ti)).alias("max_policy_premium_change"),
    )
)

tagged = (
    ranked
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        "first_policy_end_date",
        "max_policy_premium",
        "max_policy_premium_distance",
        "avg_premium_distance",
        "premium_distance_minus_avg",
        "max_policy_premium_distance_share_of_status",
        "avg_premium_distance_sum_6_prior",
        "max_policy_premium_change",
        F.round(F.col("avg_premium_distance") * 0.25, 2).alias("avg_premium_distance_adj"),
    )
)

result = (
    tagged
    .select(
        "id",
        "max_policy_premium_distance_share_of_status",
        "max_policy_premium_change",
        "avg_premium_distance_adj",
        "avg_premium_distance_sum_6_prior",
    )
    .unpivot(
        ids=["id"],
        values=["max_policy_premium_distance_share_of_status", "max_policy_premium_change", "avg_premium_distance_adj", "avg_premium_distance_sum_6_prior"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
