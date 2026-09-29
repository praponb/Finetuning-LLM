from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-10-01", "2025-03-01"))
    .select(
        "store_name",
        "sqft",
        "city",
        F.lower("city").alias("city_lower"),
        F.date_trunc("month", "opened_date").alias("opened_date_period"),
    )
)
