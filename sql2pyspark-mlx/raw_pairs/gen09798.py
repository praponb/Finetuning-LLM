from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2023-08-01")
    .select("sqft", "opened_date")
    .unionByName(
        df_stores
        .filter(F.col("region") == "North")
        .select("sqft", "opened_date")
    )
)
