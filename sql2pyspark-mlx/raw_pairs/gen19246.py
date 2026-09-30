from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["South", "East"]) & F.col("city").isin(["Berlin", "Tokyo", "London"]))
    .select("region", "opened_date", "sqft")
)
