from pyspark.sql import functions as F

prepared_adjusters = (
    df_adjusters
    .filter(F.lower(F.col("adjuster_name")).like("%ca%"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

adjuster_side = prepared_adjusters.select(F.col("id").alias("entity_id"))

holder_rows = df_policyholders.select(F.col("id").alias("entity_id"))

result = (
    adjuster_side
    .intersect(holder_rows)
    .orderBy(F.col("entity_id").desc())
)
