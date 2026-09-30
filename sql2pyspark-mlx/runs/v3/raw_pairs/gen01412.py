from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "city",
        "sqft",
        F.lower("store_name").alias("store_name_lower"),
    )
    .orderBy("sqft", F.col("store_name_lower").desc())
)
