from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_premium_ties = Window.partitionBy("status").orderBy(F.col("premium").desc_nulls_first())
w_all_by_premium = Window.orderBy("premium", "id")
w_holder_segment_product = Window.partitionBy("holder_segment", "product")
w_status_holder_id_by_end_date = Window.partitionBy("status", "holder_id").orderBy(F.col("end_date").desc_nulls_first(), "id")
w_status_by_premium_running = Window.partitionBy("status").orderBy(F.col("premium").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_policys = (
    df_policies
    .filter((F.col("premium") >= 2096.6) & (F.col("end_date") < F.lit("2026-11-25").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
    )
)

with_parent = (
    cleaned_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.segment").alias("holder_segment"),
        "p.holder_name",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

windowed = (
    with_parent
    .select(
        "id",
        "premium",
        "end_date",
        "product_clean",
        "holder_joined_on",
        F.round(F.percent_rank().over(w_status_by_premium_ties), 3).alias("premium_percent_rank"),
        F.lead("end_date").over(w_all_by_premium).alias("next_end_date"),
        F.sum("premium").over(w_holder_segment_product).alias("holder_segment_product_sum_premium"),
        F.round(F.percent_rank().over(w_status_holder_id_by_end_date), 3).alias("end_date_percent_rank"),
        F.sum("premium").over(w_status_by_premium_running).alias("running_premium"),
        F.count(F.lit(1)).over(w_holder_segment_product).alias("rows_in_holder_segment_product"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

with_stats = (
    windowed.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

result = (
    with_stats
    .select("id", "claim_count", "rows_in_holder_segment_product")
    .unpivot(
        ids=["id"],
        values=["claim_count", "rows_in_holder_segment_product"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .orderBy("id", "field")
)
