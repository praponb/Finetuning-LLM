from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "West")
    .select("id", "sqft", "opened_date")
    .unionByName(
        df_stores
        .filter(F.year("opened_date") >= 2025)
        .select("id", "sqft", "opened_date")
    )
    .orderBy("opened_date")
)
