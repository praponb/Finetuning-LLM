from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_joined_on = Window.orderBy("joined_on", "id")
w_all_by_joined_on_running = Window.orderBy("joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_region_by_joined_on = Window.partitionBy("region").orderBy(F.col("joined_on").asc_nulls_last(), "id")
w_region_by_joined_on_running = Window.partitionBy("region").orderBy(F.col("joined_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_region = Window.partitionBy("region")

filtered_holders = (
    df_policyholders
    .filter((F.col("region") != "East") & (F.year(F.col("joined_on")) == 2024))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
    )
)

windowed = (
    filtered_holders
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        F.lag("risk_score").over(w_all_by_joined_on).alias("prev_risk_score"),
        F.first("risk_score", ignorenulls=True).over(w_all_by_joined_on_running).alias("first_known_risk_score"),
        (F.col("risk_score") - F.lag("risk_score").over(w_region_by_joined_on)).alias("risk_score_change"),
        F.last("risk_score", ignorenulls=True).over(w_region_by_joined_on_running).alias("last_known_risk_score"),
        F.round(F.col("risk_score") / F.nullif(F.sum("risk_score").over(w_region), F.lit(0)), 4).alias("risk_score_share_of_region"),
    )
)

result = (
    windowed
    .groupBy("joined_on")
    .pivot("region", ["West", "South"])
    .agg(F.sum("risk_score_share_of_region"))
    .orderBy("joined_on")
)
