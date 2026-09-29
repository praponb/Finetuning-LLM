from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") < "2023-09-01") & (F.col("city") == "New York"))
    .select(
        "id",
        "region",
        "city",
        "store_name",
        F.when(F.col("region") == "East", 1).otherwise(0).alias("is_east"),
        F.initcap(F.trim("store_name")).alias("store_name_title"),
    )
    .orderBy(F.col("id").desc(), F.col("store_name_title").desc())
)
