from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.length("store_name") < 6)
    .select(
        "opened_date",
        "sqft",
        F.trim("region").alias("clean_region"),
        (F.datediff(F.current_date(), "opened_date") / 7).alias("weeks_since_opened_date"),
    )
)
