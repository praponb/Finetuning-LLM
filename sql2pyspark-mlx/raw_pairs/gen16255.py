from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.year("opened_date") >= 2025) & (F.col("region") == "East") & (F.year("opened_date") == 2024))
    .select(
        "id",
        "region",
        "sqft",
        F.substring("store_name", 1, 4).alias("store_name_prefix"),
    )
    .orderBy("region", "id")
    .limit(5)
)
