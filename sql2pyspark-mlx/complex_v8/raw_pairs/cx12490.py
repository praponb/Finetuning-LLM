from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("region").orderBy("joined_on_plus_30d", "id")

scoped_holders = (
    df_policyholders
    .filter((F.col("risk_score") >= 0.3) & (F.col("segment").isNotNull()))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "segment",
        F.date_add(F.col("joined_on"), 30).alias("joined_on_plus_30d"),
        F.lower(F.regexp_replace(F.trim(F.col("holder_name")), " ", "_")).alias("holder_name_slug"),
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled"),
    )
)

latest = (
    scoped_holders
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .rollup("region", "segment")
    .agg(
        F.grouping("region").alias("is_total_region"),
        F.round(F.sum("risk_score_filled") / F.nullif(F.sum("risk_score"), F.lit(0)), 3).alias("risk_score_filled_to_risk_score_ratio"),
        F.countDistinct("region").alias("distinct_regions"),
    )
    .select(
        F.coalesce(F.col("region"), F.lit("ALL")).alias("region"),
        F.coalesce(F.col("segment"), F.lit("ALL")).alias("segment"),
        "is_total_region",
        "risk_score_filled_to_risk_score_ratio",
        "distinct_regions",
    )
)
