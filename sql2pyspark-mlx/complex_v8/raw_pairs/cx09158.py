from pyspark.sql import functions as F

base = (
    df_policies
    .filter(F.col("status") != "LAPSED")
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.date_add(F.col("start_date"), 90).alias("start_date_plus_90d"),
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
    )
)

with_details = (
    base.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status_ref",
        "s.start_date_plus_90d",
        F.col("p.segment").alias("holder_segment"),
    )
)

stats = (
    with_details
    .groupBy("holder_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(~F.col("holder_segment").isin("VIP", "SME"), 1).otherwise(0)).alias("n_holder_segment_not_in_vip_sm"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
    )
)

annotated = (
    stats
    .filter(F.col("n_holder_segment_not_in_vip_sm") < 7)
    .select(
        "holder_id",
        "distinct_ids",
        "n_holder_segment_not_in_vip_sm",
        "distinct_holder_ids",
        F.coalesce(F.col("distinct_ids"), F.lit(0)).alias("distinct_ids_filled"),
    )
)

result = (
    annotated
    .select("holder_id", "distinct_holder_ids", "distinct_ids_filled")
    .unpivot(
        ids=["holder_id"],
        values=["distinct_holder_ids", "distinct_ids_filled"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("holder_id", "metric_name")
)
