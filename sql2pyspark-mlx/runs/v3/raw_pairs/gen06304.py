from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "New York")
    .select(
        "id",
        "region",
        "sqft",
        F.add_months("opened_date", 12).alias("opened_date_plus_months"),
    )
)
