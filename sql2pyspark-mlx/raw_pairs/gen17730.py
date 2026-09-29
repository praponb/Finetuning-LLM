from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Tokyo", "Berlin", "New York"]) | F.col("store_name").like("%Tow%"))
    .groupBy(
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        "region",
        F.quarter("opened_date").alias("opened_quarter"),
    )
    .agg(F.max("sqft").alias("max_sqft"))
)
