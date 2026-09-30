from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .select(
        F.lit("holder").alias("src"),
        F.col("id").alias("ref_id"),
        F.col("holder_name").alias("name"),
    )
)

holder_side_2 = (
    df_policyholders
    .filter((F.upper(F.trim(F.col("holder_name"))).like("E%")) & (F.col("risk_score") < 0.8))
    .select(F.lit("holder_2").alias("src"), "id", "region")
)

result = (
    holder_side
    .union(holder_side_2)
    .orderBy("name", "ref_id")
)
