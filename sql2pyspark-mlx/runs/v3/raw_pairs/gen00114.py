from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") == 13571)
    .select(
        "id",
        "store_name",
        "city",
        "region",
        F.lower("region").alias("region_lower"),
        F.date_trunc("week", "opened_date").alias("opened_date_period"),
    )
    .orderBy("region_lower", "id")
    .limit(10)
)
