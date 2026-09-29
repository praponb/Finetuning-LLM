from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~((F.col("city") != "London") & F.col("region").isin(["North", "East"])))
    .select("id", "store_name", F.lower("region").alias("region_lower"))
)
