from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Berlin")
    .select(
        "id",
        "opened_date",
        "sqft",
        "region",
        F.substring("store_name", 1, 4).alias("store_name_prefix"),
    )
    .orderBy("opened_date", "sqft", "id")
    .limit(3)
)
