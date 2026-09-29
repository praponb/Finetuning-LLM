from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_last_joined_on_full = Window.orderBy("last_joined_on", "joined_on_year", "region", "region_or_unknown").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_region_or_unknown_by_last_jo = Window.partitionBy("region_or_unknown").orderBy("last_joined_on", "joined_on_year", "region", "region_or_unknown")
w_joined_on_year_region = Window.partitionBy("joined_on_year", "region")

valid = df_policyholders.filter(F.col("segment") == "VIP")

derived = (
    valid
    .select(
        "*",
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("region_or_unknown"),
        F.year(F.col("joined_on")).alias("joined_on_year"),
    )
)

totals = (
    derived
    .groupBy("joined_on_year", "region", "region_or_unknown")
    .agg(
        F.max("joined_on").alias("last_joined_on"),
        F.array_sort(F.collect_set("segment")).alias("segment_list"),
        F.sum("risk_score").alias("sum_risk_score_check"),
    )
    .filter(F.col("sum_risk_score_check") > 0.1)
    .drop("sum_risk_score_check")
)

sequenced = (
    totals
    .select(
        "*",
        F.last("last_joined_on").over(w_all_by_last_joined_on_full).alias("last_last_joined_on"),
        F.round(F.cume_dist().over(w_region_or_unknown_by_last_jo), 3).alias("last_joined_on_cume_dist"),
        F.round(F.col("joined_on_year") / F.nullif(F.sum("joined_on_year").over(w_joined_on_year_region), F.lit(0)), 4).alias("joined_on_year_share_of_joined_on_year_region"),
        F.max("joined_on_year").over(w_joined_on_year_region).alias("joined_on_year_region_max_joined_on_year"),
    )
)

result = (
    sequenced
    .groupBy("last_last_joined_on")
    .pivot("region_or_unknown", ["UNKNOWN", "Central"])
    .agg(F.avg("joined_on_year_share_of_joined_on_year_region"))
)
