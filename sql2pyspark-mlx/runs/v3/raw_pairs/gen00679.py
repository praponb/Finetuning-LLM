from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") < "2024-05-02") & (F.col("city") != "New York"))
    .select(
        "id",
        "opened_date",
        "region",
        F.substring("store_name", 1, 3).alias("store_name_prefix"),
    )
)
