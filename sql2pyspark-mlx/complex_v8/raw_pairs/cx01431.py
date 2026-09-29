from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product").orderBy("end_date", "id")
w_dedup = Window.partitionBy("holder_segment", "holder_region").orderBy(F.col("end_date").desc(), F.col("id").desc())

recent = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-08-27").cast("date"), F.lit("2025-05-02").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.quarter(F.col("end_date")).alias("end_date_quarter"),
    )
)

without_matches = (
    recent.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status") != "LAPSED") & (F.col("p.premium").between(2013.8, 4688.7)), "left_semi")
)

unique_rows = (
    without_matches
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_details = (
    unique_rows.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.region").isin("South", "North"))
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
        F.col("p.segment").alias("holder_segment"),
    )
)

combined_details = (
    with_details.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.holder_risk_score",
        "s.holder_region",
        "s.holder_segment",
        F.col("p.segment").alias("holder_segment_2"),
        F.col("p.region").alias("holder_region_2"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

latest_per_key = (
    combined_details
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "holder_risk_score",
        "holder_region",
        "holder_segment",
        "holder_segment_2",
        "holder_region_2",
        "holder_joined_on",
    )
    .orderBy(F.col("holder_segment").asc(), "holder_risk_score")
)
