from pyspark.sql import functions as F

prepared = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.date_format(F.col("end_date"), "yyyy-MM-dd").alias("end_date_label"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.max("end_date").alias("last_policy_end_date"))
)

with_stats = (
    prepared.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.last_policy_end_date")
)

with_parent = (
    with_stats.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.upper(F.trim(F.col("p.holder_name"))).like("B%"))
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.premium_bucket",
        "p.holder_name",
        F.col("p.region").alias("holder_region"),
        F.col("p.segment").alias("holder_segment"),
    )
)

result = (
    with_parent
    .groupingSets(
        [["product", "holder_segment", "holder_region"], ["product"], []],
        "product", "holder_segment", "holder_region",
    )
    .agg(
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.datediff(F.max("start_date"), F.min("start_date")).alias("start_date_span_days"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
    )
    .orderBy(
        F.col("product").asc_nulls_last(),
        F.col("holder_segment").asc_nulls_last(),
        F.col("holder_region").asc_nulls_last(),
    )
)
