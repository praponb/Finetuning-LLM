from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "opened_date",
        "sqft",
        "region",
        F.concat_ws("-", "city", "store_name").alias("city_store_name_key"),
        F.col("sqft").cast("string").alias("sqft_str"),
    )
)
