from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_joined_on_month_by_joined_on = Window.partitionBy(F.date_trunc("month", "joined_on")).orderBy("joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_region = Window.partitionBy("region")

prepared = (
    df_policyholders
    .filter(F.lower(F.col("holder_name")).like("%nt%"))
    .select(
        "id",
        "region",
        "risk_score",
        "joined_on",
        F.substring(F.trim(F.col("segment")), 1, 2).alias("segment_prefix"),
    )
)

with_windows = (
    prepared
    .select(
        "id",
        "region",
        "risk_score",
        F.first("risk_score", ignorenulls=True).over(w_joined_on_month_by_joined_on).alias("first_known_risk_score"),
        F.round(F.avg("risk_score").over(w_region), 2).alias("region_avg_risk_score"),
        F.min("risk_score").over(w_joined_on_month_by_joined_on).alias("min_risk_score_so_far"),
    )
)

labeled = (
    with_windows
    .filter(F.col("first_known_risk_score") > 0.1)
    .select("*", F.substring(F.trim(F.col("region")), 1, 3).alias("region_prefix"))
)

result = (
    labeled
    .select(
        "id",
        "region",
        "risk_score",
        "first_known_risk_score",
        "region_avg_risk_score",
        "min_risk_score_so_far",
    )
    .orderBy("min_risk_score_so_far", F.col("region").desc())
)
