from pyspark.sql import functions as F

base = (
    df_policyholders
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.when(F.col("segment").isin("VIP", "SME"), "group_a").when(F.col("segment") == "RETAIL", "group_b").otherwise("other").alias("segment_group"),
    )
)

holder_side = (
    base
    .filter((~F.col("segment").isin("SME", "RETAIL")) & (F.col("segment_group") != "group_b"))
    .select(F.col("segment").alias("label"), "id", F.lit("holder").alias("src"))
)

adjuster_side = (
    df_adjusters
    .filter(F.col("level") < 3)
    .select(
        F.col("adjuster_name").alias("label"),
        F.col("level").alias("id"),
        F.lit("adjuster").alias("src"),
    )
)

holder_rows = (
    df_policyholders
    .filter(F.upper(F.trim(F.col("holder_name"))).like("B%"))
    .select(F.col("segment").alias("label"), "id", F.lit("holder_3").alias("src"))
)

result = (
    holder_side
    .unionByName(adjuster_side)
    .unionByName(holder_rows)
)
