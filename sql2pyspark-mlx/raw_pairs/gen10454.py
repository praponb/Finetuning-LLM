from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([4, 5, 7]) & F.col("opened_date").between("2023-07-01", "2024-08-01"))
    .select(
        "id",
        "city",
        "sqft",
        "store_name",
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
    )
    .orderBy(F.col("city").desc())
)
