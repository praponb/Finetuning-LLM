from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("region")

base = (
    df_stores
    .filter(F.col("region") == "West")
    .select(
        "*",
        F.date_add("opened_date", 48).alias("opened_date_plus_48d"),
        F.col("sqft").cast("string").alias("sqft_str"),
        F.coalesce("region", F.lit("OTHER")).alias("resolved_region"),
    )
)

result = (
    base
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "opened_date_plus_48d",
        "sqft_str",
        "resolved_region",
        F.count("sqft").over(w_ordered).alias("region_count_sqft"),
        F.min("sqft").over(w_ordered).alias("region_min_sqft"),
    )
)
