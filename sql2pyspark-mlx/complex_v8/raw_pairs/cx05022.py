from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_start_date_year_by_s = Window.partitionBy("product", F.year("start_date")).orderBy("status_len")
w_end_date_year_by_start_date = Window.partitionBy(F.year("end_date")).orderBy(F.col("start_date").desc())

cleaned_policys = (
    df_policies
    .filter((F.col("premium") > 2111.3) & (F.col("start_date") >= F.lit("2024-10-22").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

metrics = (
    cleaned_policys
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status_len",
        F.round(F.cume_dist().over(w_product_start_date_year_by_s), 3).alias("status_len_cume_dist"),
        F.round(F.cume_dist().over(w_end_date_year_by_start_date), 3).alias("start_date_cume_dist"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claimed_amount").alias("max_claim_claimed_amount"),
        F.count(F.when(~F.col("claim_type").isin("theft", "medical"), 1)).alias("claim_claim_type_count"),
        F.min("reported_at").alias("first_claim_reported_at"),
    )
)

profiled = (
    metrics.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.max_claim_claimed_amount",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.first_claim_reported_at",
    )
)

result = (
    profiled
    .groupBy("holder_id", "first_claim_reported_at")
    .pivot("product", ["AUTO", "HEALTH", "HOME"])
    .agg(F.max("start_date_cume_dist"))
    .orderBy("holder_id", "first_claim_reported_at")
)
