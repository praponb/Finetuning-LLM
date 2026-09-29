from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Tow%"))
    .select(
        "city",
        "opened_date",
        "sqft",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)
