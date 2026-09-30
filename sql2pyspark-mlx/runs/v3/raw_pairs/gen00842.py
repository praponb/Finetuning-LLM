from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["East", "West", "North"]) & F.col("opened_date").between("2023-11-01", "2024-06-01") & F.month("opened_date").isin([1, 5, 8]))
    .select(
        "id",
        "store_name",
        "region",
        "city",
        F.initcap(F.trim("region")).alias("region_title"),
        F.trim("store_name").alias("clean_store_name"),
    )
)
