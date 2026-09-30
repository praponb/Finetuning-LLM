from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_stddev_claim_type_len = Window.orderBy("stddev_claim_type_len", "claim_date_ym", "status", "adjuster_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_stddev_claim_type_len_2 = Window.orderBy("stddev_claim_type_len", "claim_date_ym", "status", "adjuster_id")
w_all_by_stddev_claim_type_len_3 = Window.orderBy("stddev_claim_type_len", "claim_date_ym", "status", "adjuster_id").rowsBetween(-6, -1)
w_all_by_stddev_claim_type_len_4 = Window.orderBy("stddev_claim_type_len")
w_n_claim_type_in_flood_collis = Window.partitionBy("n_claim_type_in_flood_collisio", "distinct_ids")

filtered = (
    df_claims
    .filter((F.col("approved_amount") > 1273.7) & (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)

qualified = (
    filtered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.status").isNotNull()), "left_semi")
)

scored = (
    qualified
    .select(
        "*",
        F.concat_ws("-", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.coalesce(F.col("approved_amount_filled"), F.lit(0.0)).alias("approved_amount_filled_filled"),
    )
)

grouped = (
    scored
    .groupBy(
        F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"),
        "status",
        "adjuster_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev("claim_type_len"), 2).alias("stddev_claim_type_len"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("claim_type").isin("flood", "collision"), 1)).alias("n_claim_type_in_flood_collisio"),
    )
)

qualified_2 = (
    grouped.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
)

with_windows = (
    qualified_2
    .select(
        "claim_date_ym",
        "status",
        "adjuster_id",
        "row_count",
        "distinct_ids",
        "n_claim_type_in_flood_collisio",
        F.last("row_count", ignorenulls=True).over(w_all_by_stddev_claim_type_len).alias("last_known_row_count"),
        (F.col("distinct_ids") - F.lag("distinct_ids").over(w_all_by_stddev_claim_type_len_2)).alias("distinct_ids_change"),
        F.sum("row_count").over(w_all_by_stddev_claim_type_len_3).alias("row_count_sum_6_prior"),
        F.dense_rank().over(w_all_by_stddev_claim_type_len_4).alias("stddev_claim_type_len_dense_rank"),
        F.round(F.col("row_count") - F.avg("row_count").over(w_n_claim_type_in_flood_collis), 2).alias("row_count_vs_n_claim_type_in_flood_collisio_dist"),
    )
)

result = (
    with_windows
    .select(
        "claim_date_ym",
        "status",
        "row_count",
        "distinct_ids",
        "n_claim_type_in_flood_collisio",
        "last_known_row_count",
        "distinct_ids_change",
        "row_count_sum_6_prior",
        "stddev_claim_type_len_dense_rank",
    )
    .orderBy("distinct_ids_change", F.col("last_known_row_count").asc_nulls_last())
)
