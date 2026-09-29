from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["North", "West", "East"]) & (F.col("opened_date") < "2025-05-01"))
    .select(
        "id",
        "opened_date",
        "store_name",
        "sqft",
        F.trim("store_name").alias("clean_store_name"),
    )
)
