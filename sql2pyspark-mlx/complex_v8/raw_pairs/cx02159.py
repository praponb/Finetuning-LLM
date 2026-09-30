from pyspark.sql import functions as F

valid = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-06-19").cast("date"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

combined_details = (
    valid.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status_len",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    combined_details
    .groupingSets(
        [["holder_region", "status_len", "holder_id"], ["holder_region"], ["status_len"]],
        "holder_region", "status_len", "holder_id",
    )
    .agg(F.count("*").alias("cnt"))
    .select(
        F.coalesce(F.col("holder_region"), F.lit("ALL")).alias("holder_region"),
        "status_len",
        "holder_id",
        "cnt",
    )
)
