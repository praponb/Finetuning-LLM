from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("region").isin(["Central", "East", "North"]))
    .select("id", "store_name")
)
