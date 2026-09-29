from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([2, 5, 9]))
    .select(
        "id",
        "city",
        "sqft",
        "opened_date",
        F.trim("region").alias("clean_region"),
        F.upper("city").alias("city_upper"),
    )
)
