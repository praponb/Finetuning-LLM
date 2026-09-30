from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~((F.col("opened_date") >= "2025-09-01") & F.col("region").isin(["North", "South"])))
    .select(
        "id",
        "sqft",
        F.concat_ws("-", "store_name", "region").alias("store_name_region_key"),
    )
    .orderBy("store_name_region_key")
)
