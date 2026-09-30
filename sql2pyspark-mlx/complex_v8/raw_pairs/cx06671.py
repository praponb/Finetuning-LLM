from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .filter(F.col("holder_name").isNotNull())
    .select(F.col("region").alias("name"), "holder_name", "id")
)

adjuster_rows = (
    df_adjusters
    .select(
        F.col("team").alias("name"),
        F.col("adjuster_name").alias("holder_name"),
        "id",
    )
)

result = (
    holder_side
    .union(adjuster_rows)
    .orderBy("id")
)
