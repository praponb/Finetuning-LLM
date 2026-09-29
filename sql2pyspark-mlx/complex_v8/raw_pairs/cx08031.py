from pyspark.sql import functions as F

base = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

policy_rows = (
    base
    .filter((F.col("holder_id").isNotNull()) & (F.col("product") == "HEALTH"))
    .select("holder_id", F.col("status").alias("name"))
)

policy_side = df_policies.select("holder_id", "product")

result = (
    policy_rows
    .intersect(policy_side)
    .orderBy(F.col("holder_id").desc())
)
