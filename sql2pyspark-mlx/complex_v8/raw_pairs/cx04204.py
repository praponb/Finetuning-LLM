from pyspark.sql import functions as F

policy_side = (
    df_policies
    .filter(F.col("status").isin("LAPSED", "EXPIRED", "ACTIVE"))
    .select(
        F.col("holder_id").alias("qty"),
        F.col("id").alias("qty_2"),
        F.lit("policy").alias("src"),
    )
)

adjuster_side = df_adjusters.select("level", "id", F.lit("adjuster").alias("src"))

policy_side_2 = (
    df_policies
    .filter(F.col("status").isin("LAPSED", "ACTIVE"))
    .select("holder_id", "id", F.lit("policy_3").alias("src"))
)

unioned = (
    policy_side
    .union(adjuster_side)
    .union(policy_side_2)
)

result = (
    unioned
    .groupBy("src")
    .agg(F.count("*").alias("n_rows"), F.sum("qty_2").alias("total_qty_2"))
    .orderBy("src")
)
