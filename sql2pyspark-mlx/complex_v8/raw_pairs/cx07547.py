from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter((F.col("holder_id") >= 16) & (F.col("start_date") >= F.lit("2024-06-19").cast("date")))
)

screened = (
    recent_policys.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") < 3465.4) & (F.col("p.product").isNotNull()), "left_semi")
)

enriched = (
    screened.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", F.col("p.region").alias("holder_region"))
)

per_group = (
    enriched
    .groupBy("product", "status")
    .agg(
        F.min("premium").alias("min_premium"),
        F.max("end_date").alias("last_end_date"),
        F.round(F.sum("premium"), 2).alias("total_premium"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("n_status_active"),
        F.sum("premium").alias("sum_premium_check"),
    )
    .filter(F.col("sum_premium_check") > 1042.8)
    .drop("sum_premium_check")
)

result = (
    per_group
    .filter(~F.col("status").isin("LAPSED", "ACTIVE"))
    .select(
        "product",
        "status",
        "min_premium",
        "last_end_date",
        "total_premium",
        "n_status_active",
    )
    .orderBy(F.col("status").desc_nulls_first(), F.col("product").asc_nulls_last())
)
