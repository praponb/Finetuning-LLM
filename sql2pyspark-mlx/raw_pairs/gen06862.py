from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Tokyo", "Bangkok"]))
    .select(
        "opened_date",
        "store_name",
        "region",
        F.round("sqft", 1).alias("sqft_rounded"),
        F.substring("region", 1, 4).alias("region_prefix"),
    )
)
