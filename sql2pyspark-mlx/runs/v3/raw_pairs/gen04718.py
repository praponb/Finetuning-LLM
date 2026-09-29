from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Chiang Mai", "Bangkok"]))
    .select("sqft", "opened_date")
    .unionByName(
        df_stores
        .filter(F.col("opened_date") < "2025-05-01")
        .select("sqft", "opened_date")
    )
    .orderBy("opened_date")
)
