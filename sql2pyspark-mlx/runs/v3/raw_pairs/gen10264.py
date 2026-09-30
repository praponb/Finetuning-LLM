from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "West")
    .select("id", "opened_date")
    .unionByName(
        df_stores
        .filter(F.col("city").isin(["New York", "Bangkok"]))
        .select("id", "opened_date")
    )
    .distinct()
)
