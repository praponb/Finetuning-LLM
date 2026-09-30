from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_premium = Window.partitionBy("product").orderBy("premium", "id")
w_status_group_by_premium_dist = Window.partitionBy("status_group").orderBy(F.col("premium_distance").desc(), "id")
w_status_by_premium_distance = Window.partitionBy("status").orderBy(F.col("premium_distance").desc(), "id")
w_product_by_premium_moving_1 = Window.partitionBy("product").orderBy("premium", "id").rowsBetween(-1, Window.currentRow)

scoped_policys = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.when(F.col("status").isin("LAPSED", "ACTIVE"), "group_a").when(F.col("status") == "EXPIRED", "group_b").otherwise("other").alias("status_group"),
        F.abs(F.col("premium") - 2319.8).alias("premium_distance"),
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
    )
)

sequenced = (
    scoped_policys
    .select(
        "*",
        (F.col("premium") - F.lag("premium").over(w_product_by_premium)).alias("premium_change"),
        F.round(F.cume_dist().over(w_status_group_by_premium_dist), 3).alias("premium_distance_cume_dist"),
        F.lag("premium").over(w_status_by_premium_distance).alias("prev_premium"),
        (F.col("premium") - F.lag("premium").over(w_status_by_premium_distance)).alias("premium_change_2"),
        F.round(F.avg("premium").over(w_product_by_premium_moving_1), 2).alias("premium_avg_2"),
    )
)

labeled = (
    sequenced
    .filter(F.col("premium_change") <= 500.0)
    .select(
        "*",
        (F.floor(F.col("premium_avg_2") / 1000) * 1000).alias("premium_avg_2_bucket"),
        F.when(F.col("premium") >= 3544.1, "A").when(F.col("premium") >= 2289.0, "B").otherwise("C").alias("premium_tier"),
        F.concat(F.col("status_group"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_group_ref"),
    )
)

with_details = (
    labeled.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.status",
        "s.status_group",
        "s.status_prefix",
        "s.premium_change",
        "s.premium_distance_cume_dist",
        "s.prev_premium",
        "s.premium_change_2",
        "s.premium_avg_2",
        "s.premium_avg_2_bucket",
        "s.status_group_ref",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
        "p.holder_name",
    )
)

with_activity = (
    with_details.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left_semi")
)

result = (
    with_activity
    .rollup("status", "status_group")
    .agg(F.min("prev_premium").alias("min_prev_premium"))
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        F.coalesce(F.col("status_group"), F.lit("ALL")).alias("status_group"),
        "min_prev_premium",
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("status_group").asc_nulls_last(),
    )
)
