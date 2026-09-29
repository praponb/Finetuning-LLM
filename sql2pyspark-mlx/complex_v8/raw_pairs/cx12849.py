from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_reported_at_r = Window.partitionBy("adjuster_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_reported_at_ties = Window.orderBy(F.col("reported_at").desc())
w_adjuster_id_by_reported_at_f = Window.partitionBy("adjuster_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_rank_claim_type = Window.partitionBy("reported_at_rank", "claim_type_list_item").orderBy(F.col("distinct_claim_types").desc(), "claim_type", "reported_at_rank", "claim_date_month", "claim_type_list_item")
w_claim_type = Window.partitionBy("claim_type")
w_n_claim_type_is_not_null_cla = Window.partitionBy("n_claim_type_is_not_null", F.date_trunc("month", "claim_date_month")).orderBy("n_claim_type_is_not_null", "claim_type", "reported_at_rank", "claim_date_month", "claim_type_list_item")
w_claim_type_claim_type_list_i = Window.partitionBy("claim_type", "claim_type_list_item").orderBy(F.col("distinct_claim_types").desc(), "claim_type", "reported_at_rank", "claim_date_month", "claim_type_list_item").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_claims
    .filter((F.col("approved_amount") < 32510.7) & (F.col("claim_date") < F.lit("2025-04-17").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
    )
)

ranked = (
    cleaned
    .select(
        "id",
        "claim_date",
        "claim_type",
        F.first("claim_date", ignorenulls=True).over(w_adjuster_id_by_reported_at_r).alias("first_known_claim_date"),
        F.round(F.percent_rank().over(w_all_by_reported_at_ties), 3).alias("reported_at_percent_rank"),
        F.rank().over(w_all_by_reported_at_ties).alias("reported_at_rank"),
        F.last("reported_at").over(w_adjuster_id_by_reported_at_f).alias("last_reported_at"),
    )
)

summary = (
    ranked
    .groupBy(
        "claim_type",
        "reported_at_rank",
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
    )
    .agg(
        F.countDistinct("claim_type").alias("distinct_claim_types"),
        F.count(F.when(F.col("claim_type").isNotNull(), 1)).alias("n_claim_type_is_not_null"),
        F.sum("reported_at_percent_rank").alias("total_reported_at_percent_rank"),
        F.max("reported_at_percent_rank").alias("max_reported_at_percent_rank"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
    )
)

per_label = (
    summary
    .select(
        "claim_type",
        "reported_at_rank",
        "claim_date_month",
        "distinct_claim_types",
        "n_claim_type_is_not_null",
        "total_reported_at_percent_rank",
        "max_reported_at_percent_rank",
        F.explode("claim_type_list").alias("claim_type_list_item"),
    )
)

screened = (
    per_label
    .crossJoin(per_label.agg(F.avg("n_claim_type_is_not_null").alias("overall_avg_n_claim_type_is_not_null")))
    .filter(F.col("n_claim_type_is_not_null") > F.col("overall_avg_n_claim_type_is_not_null"))
    .drop("overall_avg_n_claim_type_is_not_null")
)

windowed = (
    screened
    .select(
        "claim_type",
        "reported_at_rank",
        "claim_date_month",
        "distinct_claim_types",
        "n_claim_type_is_not_null",
        "max_reported_at_percent_rank",
        "claim_type_list_item",
        F.lag("claim_date_month", 3).over(w_reported_at_rank_claim_type).alias("prev3_claim_date_month"),
        F.round(F.col("distinct_claim_types") / F.nullif(F.sum("distinct_claim_types").over(w_claim_type), F.lit(0)), 4).alias("distinct_claim_types_share_of_claim_type"),
        F.lead("n_claim_type_is_not_null").over(w_n_claim_type_is_not_null_cla).alias("next_n_claim_type_is_not_null"),
        F.last("n_claim_type_is_not_null", ignorenulls=True).over(w_claim_type_claim_type_list_i).alias("last_known_n_claim_type_is_not_null"),
        F.ntile(10).over(w_reported_at_rank_claim_type).alias("distinct_claim_types_decile"),
    )
)

scored = (
    windowed
    .select(
        "claim_type",
        "reported_at_rank",
        "claim_date_month",
        "n_claim_type_is_not_null",
        "claim_type_list_item",
        "prev3_claim_date_month",
        "distinct_claim_types_share_of_claim_type",
        "next_n_claim_type_is_not_null",
        "last_known_n_claim_type_is_not_null",
        "distinct_claim_types_decile",
        F.abs(F.col("distinct_claim_types") - 6).alias("distinct_claim_types_distance"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("distinct_claim_types").cast("string")).alias("claim_type_ref"),
    )
)

result = (
    scored
    .rollup("next_n_claim_type_is_not_null", "claim_type")
    .agg(
        F.min("reported_at_rank").alias("min_reported_at_rank"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.round(F.avg("distinct_claim_types_share_of_claim_type"), 2).alias("avg_distinct_claim_types_share_of_claim_type"),
    )
)
