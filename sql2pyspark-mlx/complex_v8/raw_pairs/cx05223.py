from pyspark.sql import functions as F

policy_side = (
    df_policies
    .filter(F.col("status") != "LAPSED")
    .select(F.col("holder_id").alias("entity_id"), "product")
)

adjuster_side = (
    df_adjusters
    .filter((F.col("team").isin("Property", "Motor")) & (F.upper(F.trim(F.col("adjuster_name"))).like("E%")))
    .select("level", "team")
)

result = (
    policy_side
    .intersect(adjuster_side)
    .orderBy("product", "entity_id")
)
