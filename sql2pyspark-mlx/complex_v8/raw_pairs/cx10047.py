from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_end_date = Window.orderBy(F.col("end_date").desc(), "id")
w_status = Window.partitionBy("status")
w_first_policy_start_date_mont = Window.partitionBy(F.month("first_policy_start_date"), "product").orderBy("premium", "id")

recent = (
    df_policies
    .filter((F.col("status") == "CANCELLED") | (F.col("end_date").between(F.lit("2025-06-08").cast("date"), F.lit("2026-12-05").cast("date"))))
)

with_details = (
    recent.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.status",
        F.col("p.region").alias("holder_region"),
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.min("start_date").alias("first_policy_start_date"),
    )
)

summarized_rows = (
    with_details.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        "p.first_policy_start_date",
    )
)

with_trends = (
    summarized_rows
    .select(
        "*",
        F.row_number().over(w_all_by_end_date).alias("rn"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.ntile(5).over(w_all_by_end_date).alias("end_date_quintile"),
        F.row_number().over(w_first_policy_start_date_mont).alias("rn_2"),
        F.rank().over(w_first_policy_start_date_mont).alias("premium_rank"),
        F.dense_rank().over(w_all_by_end_date).alias("end_date_dense_rank"),
    )
)

result = (
    with_trends
    .select("id", "end_date_dense_rank", "rn")
    .unpivot(
        ids=["id"],
        values=["end_date_dense_rank", "rn"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
