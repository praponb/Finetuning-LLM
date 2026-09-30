from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-07-06").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.concat_ws("-", "region", "store_name").alias("region_store_name_key"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_filled"),
    )
)

result = (
    filtered
    .filter(F.col("city") == "New York")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "region_store_name_key",
        "region_filled",
        F.avg("sqft").over(w).alias("running_avg_sqft"),
        F.sum("sqft").over(w).alias("running_sum_sqft"),
        F.min("sqft").over(w).alias("running_min_sqft"),
    )
)
