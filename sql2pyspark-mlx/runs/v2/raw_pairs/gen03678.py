from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["North", "Central"]) & F.upper("store_name").like("%ALL%"))
    .select(
        "store_name",
        "region",
        "city",
        F.dayofmonth("opened_date").alias("opened_date_day"),
    )
)
