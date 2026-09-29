from pyspark.sql import functions as F

prepared = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)

screened = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (~F.col("p.region").isin("East", "South")), "left_anti")
)

labeled = (
    screened
    .select(
        "*",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.date_add(F.col("start_date"), 30).alias("start_date_plus_30d"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

result = (
    labeled
    .groupingSets(
        [["product", "holder_id", "status"], ["product"]],
        "product", "holder_id", "status",
    )
    .agg(F.round(F.stddev("premium"), 2).alias("stddev_premium"))
    .select(
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "holder_id",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "stddev_premium",
    )
    .orderBy(
        F.col("product").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
