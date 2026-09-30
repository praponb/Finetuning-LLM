from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_regions_region_by_d = Window.partitionBy("distinct_regions", "region").orderBy(F.col("distinct_regions").desc(), "region", "segment")
w_distinct_regions_region_by_d_2 = Window.partitionBy("distinct_regions", "region").orderBy(F.col("distinct_regions").desc())

scoped_holders = (
    df_policyholders
    .filter(F.col("risk_score") <= 0.6)
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "segment",
    )
)

totals = (
    scoped_holders
    .groupBy("region", "segment")
    .agg(
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
        F.countDistinct("region").alias("distinct_regions"),
        F.max("risk_score").alias("max_risk_score_check"),
    )
    .filter(F.col("max_risk_score_check") > 0.3)
    .drop("max_risk_score_check")
)

with_trends = (
    totals
    .select(
        "region",
        "segment",
        "distinct_regions",
        F.lead("avg_risk_score").over(w_distinct_regions_region_by_d).alias("next_avg_risk_score"),
        F.round(F.percent_rank().over(w_distinct_regions_region_by_d_2), 3).alias("distinct_regions_percent_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("next_avg_risk_score") > 0.4)
    .select(
        "region",
        "segment",
        "distinct_regions",
        "next_avg_risk_score",
        F.lower(F.regexp_replace(F.trim(F.col("region")), " ", "_")).alias("region_slug"),
    )
    .orderBy(
        F.col("region_slug").desc_nulls_first(),
        F.col("region").asc_nulls_last(),
    )
)
