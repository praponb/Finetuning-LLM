from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "opened_date",
        "store_name",
        F.when(F.col("city") == "Singapore", 1).otherwise(0).alias("is_singapore"),
        F.ceil("sqft").alias("sqft_whole"),
    )
)
