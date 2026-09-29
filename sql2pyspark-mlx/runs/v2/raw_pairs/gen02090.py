from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["Central", "East"]))
    .select("id", "opened_date")
    .unionByName(
        df_stores
        .filter(F.col("sqft").between(4615, 8002))
        .select("id", "opened_date")
    )
    .distinct()
    .orderBy("id")
)
