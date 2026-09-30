from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") <= "2024-01-01") | F.col("store_name").like("%Kiosk%"))
    .select(
        "id",
        "opened_date",
        "city",
        F.when(F.col("city") == "New York", 1).otherwise(0).alias("is_new_york"),
    )
    .orderBy(F.col("is_new_york").desc())
)
