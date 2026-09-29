from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.lower(F.trim("store_name")) == "airport kiosk") & F.col("store_name").like("%Exp%") & (F.col("sqft") <= 10229))
    .select(
        "sqft",
        "opened_date",
        "city",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
    .orderBy(F.col("sqft").desc(), F.col("days_since_opened_date").desc())
)
