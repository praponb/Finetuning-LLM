from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_status_active_status_by_st = Window.partitionBy("n_status_active", "status").orderBy("stddev_premium", "status", "holder_id", "start_date_ym").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_status_active_status_by_pr = Window.partitionBy("n_status_active", "status").orderBy("premium_status_in_lapsed_expired")

scoped_policys = (
    df_policies
    .filter(F.col("status").isin("ACTIVE", "CANCELLED", "EXPIRED"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
    )
)

per_group = (
    scoped_policys
    .filter(F.col("start_date") >= F.lit("2024-08-27").cast("date"))
    .groupBy(
        "status",
        "holder_id",
        F.date_format("start_date", "yyyy-MM").alias("start_date_ym"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("status").isin("EXPIRED", "ACTIVE", "LAPSED"), 1)) / F.count("*"), 1).alias("pct_status_in_expired_active_lap"),
        F.min("premium").alias("min_premium"),
        F.round(F.stddev("premium"), 2).alias("stddev_premium"),
        F.sum(F.when(F.col("status") != "ACTIVE", 1).otherwise(0)).alias("n_status_active"),
        F.sum(F.when(F.col("status").isin("LAPSED", "EXPIRED"), F.col("premium"))).alias("premium_status_in_lapsed_expired"),
    )
)

sequenced = (
    per_group
    .select(
        "status",
        "holder_id",
        "start_date_ym",
        "min_premium",
        "stddev_premium",
        F.last("n_status_active").over(w_n_status_active_status_by_st).alias("last_n_status_active"),
        F.round(F.cume_dist().over(w_n_status_active_status_by_pr), 3).alias("premium_status_in_lapsed_expired_cume_dist"),
    )
)

combined_details = (
    sequenced.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.joined_on") >= F.lit("2024-09-04").cast("date"))
    .select(
        "s.status",
        "s.holder_id",
        "s.start_date_ym",
        "s.stddev_premium",
        "s.last_n_status_active",
        "s.premium_status_in_lapsed_expired_cume_dist",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

enriched = (
    combined_details.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.status",
        "s.holder_id",
        "s.start_date_ym",
        "s.stddev_premium",
        "s.holder_joined_on",
        F.col("p.segment").alias("holder_segment"),
    )
)

result = (
    enriched
    .select(
        "status",
        "holder_id",
        "start_date_ym",
        "stddev_premium",
        "holder_joined_on",
        "holder_segment",
    )
    .orderBy(F.col("holder_segment").desc())
)
