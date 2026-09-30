from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") <= 15114) | (F.col("sqft") < 5272))
    .select(
        "opened_date",
        "sqft",
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
        F.trim("store_name").alias("clean_store_name"),
    )
)
