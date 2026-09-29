from pyspark.sql import functions as F

valid_holders = (
    df_policyholders
    .filter((F.col("region").isin("South", "East", "North")) & (F.col("risk_score") > 0.2))
    .select(
        "id",
        "risk_score",
        "joined_on",
        "segment",
        F.round(F.col("risk_score") * 1.5, 2).alias("risk_score_adj"),
    )
)

holder_side = (
    df_policyholders
    .select(
        F.lit("holder").alias("record_type"),
        "segment",
        F.col("region").alias("label"),
        F.col("id").alias("qty"),
    )
)

adjuster_side = (
    df_adjusters
    .filter((F.col("level") > 1) & (~F.col("team").isin("Health", "Travel")))
    .select(
        F.lit("adjuster").alias("record_type"),
        "team",
        "adjuster_name",
        "id",
    )
)

unioned = (
    holder_side
    .union(adjuster_side)
    .distinct()
)

result = (
    unioned
    .groupBy("record_type")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("record_type")
)
