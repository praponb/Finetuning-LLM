from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_joined_on_month = Window.partitionBy("joined_on_month")
w_region_by_risk_score_ties = Window.partitionBy("region").orderBy(F.col("risk_score").desc())

prepared_holders = (
    df_policyholders
    .filter((F.col("segment") == "RETAIL") & (F.col("joined_on").between(F.lit("2024-02-04").cast("date"), F.lit("2025-10-01").cast("date"))))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "segment",
        F.concat(F.col("region"), F.lit("#"), F.col("id").cast("string")).alias("region_ref"),
        F.month(F.col("joined_on")).alias("joined_on_month"),
    )
)

with_windows = (
    prepared_holders
    .select(
        "id",
        "holder_name",
        "risk_score",
        "segment",
        "joined_on_month",
        F.min("risk_score").over(w_joined_on_month).alias("joined_on_month_min_risk_score"),
        F.rank().over(w_region_by_risk_score_ties).alias("risk_score_rank"),
        F.round(F.cume_dist().over(w_region_by_risk_score_ties), 3).alias("risk_score_cume_dist"),
    )
)

derived = (
    with_windows
    .select(
        "id",
        "holder_name",
        "risk_score",
        "segment",
        "risk_score_cume_dist",
        F.when(F.col("risk_score_rank") >= 8, "HIGH").when(F.col("risk_score_rank") >= 5, "MEDIUM").otherwise("LOW").alias("risk_score_rank_tier"),
    )
)

result = (
    derived
    .select(
        "id",
        "holder_name",
        "segment",
        "risk_score_cume_dist",
        "risk_score_rank_tier",
        F.greatest(F.col("risk_score_cume_dist"), F.col("risk_score")).alias("greatest_risk_score_cume_dist_risk_score"),
    )
    .orderBy("id", F.col("risk_score_rank_tier").desc())
)
