from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "North") & (F.col("opened_date") > "2024-09-01"))
    .orderBy(F.col("sqft").desc(), "region", "id")
    .limit(10)
)
