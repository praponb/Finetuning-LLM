from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .filter(F.col("holder_name").isNotNull())
    .select(
        F.lit("holder").alias("record_type"),
        F.col("segment").alias("category"),
        "joined_on",
    )
)

policy_side = (
    df_policies
    .filter((F.col("end_date").isNotNull()) & (~F.col("product").isin("HEALTH", "AUTO")))
    .select(F.lit("policy").alias("record_type"), "product", "end_date")
)

all_events = holder_side.union(policy_side)

result = (
    all_events
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("joined_on").alias("latest_joined_on"),
    )
    .orderBy("record_type")
)
