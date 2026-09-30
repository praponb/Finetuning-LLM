from pyspark.sql import functions as F

base_adjusters = (
    df_adjusters
    .filter(F.upper(F.trim(F.col("adjuster_name"))).like("B%"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.length(F.trim(F.col("adjuster_name"))).alias("adjuster_name_len"),
    )
)

adjuster_side = base_adjusters.select(F.col("id").alias("ref_id"))

holder_side = (
    df_policyholders
    .filter(F.col("region") != "South")
    .select("id")
)

result = (
    adjuster_side
    .intersect(holder_side)
    .orderBy(F.col("ref_id").desc())
)
