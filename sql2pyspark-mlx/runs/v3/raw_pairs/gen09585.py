from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "West")
    .select("id", "sqft", "opened_date")
    .unionByName(
        df_stores
        .filter(F.col("sqft").isNull())
        .select("id", "sqft", "opened_date")
    )
    .orderBy("sqft")
)
