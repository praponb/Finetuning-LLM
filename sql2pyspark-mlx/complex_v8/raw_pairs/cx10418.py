from pyspark.sql import functions as F

base_policys = (
    df_policies
    .filter(F.col("status") != "EXPIRED")
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

tagged = (
    base_policys
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        "premium_bucket",
        "premium_filled",
        F.round(F.col("premium_filled") * 0.25, 2).alias("premium_filled_adj"),
        F.dayofweek(F.col("end_date")).alias("end_date_dayofweek"),
    )
)

result = (
    tagged
    .groupingSets(
        [["holder_id", "status", "end_date_dayofweek"], ["holder_id"], ["status"], []],
        "holder_id", "status", "end_date_dayofweek",
    )
    .agg(
        F.grouping("holder_id").alias("is_total_holder_id"),
        F.max("end_date_dayofweek").alias("max_end_date_dayofweek"),
        F.round(100 * F.count(F.when(F.col("status") == "EXPIRED", 1)) / F.count("*"), 1).alias("pct_status_expired"),
    )
)
