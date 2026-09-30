from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") <= 4494)
    .select("sqft")
    .unionByName(
        df_stores
        .filter(~F.col("region").isin(["Central", "North", "South"]))
        .select("sqft")
    )
    .orderBy("sqft")
)
