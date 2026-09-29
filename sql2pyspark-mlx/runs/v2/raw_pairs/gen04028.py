from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.year("opened_date") >= 2024)
    .select(
        "id",
        "opened_date",
        "region",
        F.trim("store_name").alias("clean_store_name"),
        F.add_months("opened_date", 3).alias("opened_date_plus_months"),
    )
)
