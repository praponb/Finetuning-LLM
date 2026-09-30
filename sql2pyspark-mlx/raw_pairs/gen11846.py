from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "East") | (F.length("store_name") < 13))
    .select(
        "sqft",
        "store_name",
        F.round("sqft", 1).alias("sqft_rounded"),
        F.date_format("opened_date", "yyyy-MM").alias("opened_date_label"),
    )
)
