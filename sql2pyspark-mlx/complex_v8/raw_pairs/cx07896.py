from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter(F.col("holder_id") >= 14)
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

enriched = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.status",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.region").alias("holder_region"),
        "p.holder_name",
    )
)

result = (
    enriched
    .groupingSets(
        [["status", "product", "holder_id"], ["status"], []],
        "status", "product", "holder_id",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("holder_name").isNotNull(), 1)) / F.count("*"), 1).alias("pct_holder_name_is_not_null"),
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
