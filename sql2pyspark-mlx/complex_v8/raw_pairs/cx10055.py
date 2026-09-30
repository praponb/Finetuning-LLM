from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((F.col("claim_type") != "theft") & (F.col("claimed_amount").between(4554.8, 30115.9)))
    .select(
        F.col("id").alias("entity_id"),
        F.col("claim_date").alias("day"),
        F.col("status").alias("label"),
        F.lit("claim").alias("record_type"),
    )
)

claim_side_2 = (
    df_claims
    .filter((F.col("claim_date") >= F.lit("2024-06-20").cast("date")) & (F.col("adjuster_id").isNotNull()))
    .select(
        "adjuster_id",
        "claim_date",
        "status",
        F.lit("claim_2").alias("record_type"),
    )
)

claim_side_3 = (
    df_claims
    .filter((F.col("claimed_amount").between(6745.1, 40639.7)) & (F.year(F.col("claim_date")) == 2024))
    .select(
        "id",
        "claim_date",
        "status",
        F.lit("claim_3").alias("record_type"),
    )
)

all_events = (
    claim_side
    .union(claim_side_2)
    .union(claim_side_3)
)

result = (
    all_events
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
        F.max("day").alias("latest_day"),
    )
    .orderBy("record_type")
)
