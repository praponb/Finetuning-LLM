from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2024-08-01")
    .select("region", "sqft")
    .unionByName(
        df_stores
        .filter(F.col("sqft").between(7737, 12200))
        .select("region", "sqft")
    )
)
