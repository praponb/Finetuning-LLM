from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.year("opened_date") == 2025)
    .select("id", "sqft", "store_name")
    .subtract(
        df_stores
        .filter(F.col("region") == "West")
        .select("id", "sqft", "store_name")
    )
)
