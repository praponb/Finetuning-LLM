from pyspark.sql import functions as F

policy_rows = (
    df_policies
    .filter((F.col("product").isNotNull()) & (~F.col("status").isin("LAPSED", "CANCELLED")))
    .select(
        F.col("id").alias("entity_id"),
        "holder_id",
        F.col("status").alias("category"),
        F.lit("policy").alias("src"),
    )
)

policy_rows_2 = (
    df_policies
    .filter(F.col("premium") <= 4670.9)
    .select(
        "id",
        "holder_id",
        "status",
        F.lit("policy_2").alias("src"),
    )
)

adjuster_side = (
    df_adjusters
    .filter((F.lower(F.col("adjuster_name")).like("% s%")) & (F.col("team") == "Health"))
    .select(
        "id",
        "level",
        "adjuster_name",
        F.lit("adjuster").alias("src"),
    )
)

result = (
    policy_rows
    .union(policy_rows_2)
    .union(adjuster_side)
    .orderBy(F.col("src").desc())
)
