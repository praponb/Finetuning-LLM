from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("region", "city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_stores
    .filter(F.col("region").isin(["West", "South"]))
    .select(
        "*",
        F.coalesce("store_name", F.lit("unassigned")).alias("store_name_filled"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_filled"),
    )
)

result = (
    base
    .filter(F.col("city").isin(["London", "Singapore", "Chiang Mai"]))
    .select(
        "id",
        "city",
        "region",
        "store_name_filled",
        F.sum("sqft").over(w_hist).alias("running_sum_sqft"),
        F.max("sqft").over(w_hist).alias("running_max_sqft"),
        F.min("sqft").over(w_hist).alias("running_min_sqft"),
        F.avg("sqft").over(w_hist).alias("running_avg_sqft"),
    )
)
