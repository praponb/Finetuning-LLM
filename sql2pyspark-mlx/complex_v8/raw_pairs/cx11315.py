from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_end_date_month_holder_id_by = Window.partitionBy(F.date_trunc("month", "end_date"), "holder_id").orderBy(F.col("end_date").desc(), "id")

prepared_policys = (
    df_policies
    .filter((F.col("premium") < 2945.9) | (F.col("end_date") >= F.lit("2025-10-12").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.count(F.when(F.col("claim_type").isin("medical", "delay"), 1)).alias("claim_claim_type_count"),
        F.count("*").alias("claim_count"),
    )
)

summarized_rows = (
    prepared_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.avg_claim_approved_amount",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

metrics = (
    summarized_rows
    .select(
        "*",
        F.min("claim_claim_type_count").over(w_product).alias("product_min_claim_claim_type_count"),
        F.round(F.percent_rank().over(w_end_date_month_holder_id_by), 3).alias("end_date_percent_rank"),
        F.round(F.col("avg_claim_approved_amount") - F.avg("avg_claim_approved_amount").over(w_product), 2).alias("avg_claim_approved_amount_vs_product_avg"),
    )
)

result = (
    metrics
    .select(
        "id",
        "claim_count",
        "product_min_claim_claim_type_count",
        "claim_claim_type_count",
    )
    .unpivot(
        ids=["id"],
        values=["claim_count", "product_min_claim_claim_type_count", "claim_claim_type_count"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
