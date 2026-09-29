from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("segment", "region").orderBy("risk_score", "id")

valid_holders = (
    df_policyholders
    .filter(F.col("joined_on").between(F.lit("2024-02-04").cast("date"), F.lit("2025-10-17").cast("date")))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.substring(F.trim(F.col("region")), 1, 3).alias("region_prefix"),
        F.substring(F.trim(F.col("holder_name")), 1, 3).alias("holder_name_prefix"),
    )
)

filtered = (
    valid_holders.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.status").isin("LAPSED", "CANCELLED")) & (F.year(F.col("p.start_date")) == 2024), "left_anti")
    .filter(F.lower(F.col("holder_name")).like("%er%"))
)

without_matches = (
    filtered
    .crossJoin(filtered.agg(F.max("risk_score").alias("overall_max_risk_score")))
    .filter(F.col("risk_score") < F.col("overall_max_risk_score"))
    .drop("overall_max_risk_score")
)

unique_rows = (
    without_matches
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

labeled = (
    unique_rows
    .filter(F.col("holder_name_prefix").isNotNull())
    .select(
        "*",
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled"),
        F.date_trunc("quarter", F.col("joined_on")).alias("joined_on_quarter"),
        F.lower(F.regexp_replace(F.trim(F.col("segment")), " ", "_")).alias("segment_slug"),
    )
)

screened = (
    labeled.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left_semi")
)

result = (
    screened
    .groupBy("joined_on", "joined_on_quarter")
    .pivot("segment", ["SME", "VIP", "RETAIL"])
    .agg(F.avg("risk_score_filled").alias("avg_v"), F.max("risk_score_filled").alias("max_v"))
    .orderBy("joined_on", "joined_on_quarter")
)
