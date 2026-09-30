from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2024-02-01", "2024-12-01"))
    .select(
        "opened_date",
        "sqft",
        "region",
        F.concat_ws("/", "city", "store_name").alias("city_store_name_key"),
    )
    .orderBy(F.col("sqft").desc())
)
