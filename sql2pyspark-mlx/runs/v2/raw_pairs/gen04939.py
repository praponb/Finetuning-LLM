from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "Central") & F.month("opened_date").isin([2, 3, 9]) & F.month("opened_date").isin([1, 2, 7]))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        F.initcap(F.trim("city")).alias("city_title"),
        F.concat("store_name", F.lit(" - "), "region").alias("store_name_region"),
    )
)
