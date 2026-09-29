from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNull() & F.col("city").isin(["Bangkok", "New York", "Singapore"]) & (F.col("region") != "North"))
    .select("id", "store_name", F.coalesce("sqft", F.lit(0)).alias("sqft_filled"))
)
