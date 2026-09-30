from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product = Window.partitionBy("holder_id", "product")
w_holder_id_by_end_date_full = Window.partitionBy("holder_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_end_date_runnin = Window.partitionBy("holder_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium_filled_2 = Window.orderBy(F.col("premium_filled_2").desc(), "id")
w_all_by_premium_filled_2_ties = Window.orderBy(F.col("premium_filled_2").desc())
w_latest = Window.partitionBy("holder_id", "product").orderBy(F.col("premium_filled_2_rank").desc(), F.col("id").desc())

filtered = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("product"))).alias("product_len"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

tagged = (
    filtered
    .select(
        "*",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled_2"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        F.date_format(F.col("end_date"), "yyyy-MM-dd").alias("end_date_label"),
    )
)

metrics = (
    tagged
    .select(
        "*",
        F.round(F.col("days_since_end_date") - F.avg("days_since_end_date").over(w_holder_id_product), 2).alias("days_since_end_date_vs_holder_id_product_avg"),
        F.last("product_len").over(w_holder_id_by_end_date_full).alias("last_product_len"),
        F.sum("product_len").over(w_holder_id_by_end_date_runnin).alias("running_product_len"),
        F.rank().over(w_all_by_premium_filled_2).alias("premium_filled_2_rank"),
        F.round(F.percent_rank().over(w_all_by_premium_filled_2_ties), 3).alias("premium_filled_2_percent_rank"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.sum("approved_amount").alias("total_claim_approved_amount"),
        F.count("*").alias("claim_count"),
    )
)

summarized_rows = (
    metrics.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.total_claim_approved_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

latest_per_key = (
    summarized_rows
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select(
        "id",
        "premium",
        "start_date",
        "end_date",
        "product_len",
        "premium_filled",
        "days_since_end_date",
        "premium_filled_2_rank",
        "premium_filled_2_percent_rank",
        "total_claim_approved_amount",
        "claim_count",
    )
    .orderBy("premium_filled_2_percent_rank", F.col("product_len").desc())
)
