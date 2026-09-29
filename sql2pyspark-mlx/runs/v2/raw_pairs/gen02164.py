from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("store_name").like("Ma%"))
    .select(
        "id",
        "region",
        "opened_date",
        "store_name",
        F.ceil("sqft").alias("sqft_whole"),
    )
)
