from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "London")
    .select("id", "region", "store_name")
    .subtract(
        df_stores
        .filter(~F.col("city").isin(["New York", "Chiang Mai", "Bangkok"]))
        .select("id", "region", "store_name")
    )
    .orderBy(F.col("region").desc())
)
