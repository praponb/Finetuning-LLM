from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id").orderBy(F.col("premium_distance").desc(), F.col("id").desc())
w_status = Window.partitionBy("status")
w_end_date_year_by_start_date = Window.partitionBy(F.year("end_date")).orderBy("start_date_month_end", "id")
w_product_end_date_year_by_rn = Window.partitionBy("product", F.year("end_date")).orderBy("rn", "id").rowsBetween(-1, -1)
w_product_end_date_year_by_rn_2 = Window.partitionBy("product", F.year("end_date")).orderBy("rn", "id")
w_product_end_date_year_by_rn_3 = Window.partitionBy("product", F.year("end_date")).orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product = Window.partitionBy("product")

prepared = (
    df_policies
    .filter((F.col("status").isNotNull()) & (F.col("premium") >= 2033.8))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
        F.abs(F.col("premium") - 2289.0).alias("premium_distance"),
        F.last_day(F.col("start_date")).alias("start_date_month_end"),
    )
)

latest_per_key = (
    prepared
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_activity = (
    latest_per_key.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type").isin("flood", "delay")) & (F.hour(F.col("c.reported_at")).between(8, 14)), "left_semi")
    .filter(F.col("holder_id").isNotNull())
)

windowed = (
    with_activity
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        "product_clean",
        "premium_distance",
        "start_date_month_end",
        F.round(F.col("premium_distance") - F.avg("premium_distance").over(w_status), 2).alias("premium_distance_vs_status_avg"),
        F.row_number().over(w_end_date_year_by_start_date).alias("rn"),
        F.size(F.collect_set("product").over(w_status)).alias("distinct_product_in_status"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.round(F.avg("premium"), 2).alias("avg_policy_premium"))
)

summarized_rows = (
    windowed.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.avg_policy_premium")
)

sequenced = (
    summarized_rows
    .select(
        "*",
        F.round(F.avg("premium_distance").over(w_product_end_date_year_by_rn), 2).alias("premium_distance_avg_1_prior"),
        F.row_number().over(w_product_end_date_year_by_rn_2).alias("rn_2"),
        F.max("rn").over(w_product_end_date_year_by_rn_3).alias("max_rn_so_far"),
        F.first("rn", ignorenulls=True).over(w_product_end_date_year_by_rn_3).alias("first_known_rn"),
        F.round(F.col("premium_distance") / F.nullif(F.sum("premium_distance").over(w_product), F.lit(0)), 4).alias("premium_distance_share_of_product"),
    )
)

enriched = (
    sequenced
    .select(
        "*",
        F.last_day(F.col("end_date")).alias("end_date_month_end"),
        F.coalesce(F.col("rn_2"), F.lit(0)).alias("rn_2_filled"),
    )
)

result = (
    enriched
    .groupingSets(
        [["rn", "status", "rn_2"], ["rn"], ["status"], []],
        "rn", "status", "rn_2",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.sum("rn") / F.nullif(F.sum("rn_2_filled"), F.lit(0)), 3).alias("rn_to_rn_2_filled_ratio"),
    )
    .orderBy(
        F.col("rn").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("rn_2").asc_nulls_last(),
    )
)
