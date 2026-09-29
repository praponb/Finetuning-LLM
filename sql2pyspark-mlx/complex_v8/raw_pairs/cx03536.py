from pyspark.sql import functions as F

scoped = (
    df_adjusters
    .filter((F.col("team").isNotNull()) & (F.length(F.trim(F.col("adjuster_name"))) >= 5))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
    )
)

adjuster_rows = (
    scoped
    .filter((F.col("level") < 3) & (F.col("team") != "Travel"))
    .select(
        F.lit("adjuster").alias("record_type"),
        "level_filled",
        "id",
        "adjuster_name",
    )
)

policy_rows = (
    df_policies
    .select(
        F.lit("policy").alias("record_type"),
        "id",
        "holder_id",
        "product",
    )
)

unioned = adjuster_rows.union(policy_rows)

result = (
    unioned
    .groupBy("record_type")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("record_type")
)
