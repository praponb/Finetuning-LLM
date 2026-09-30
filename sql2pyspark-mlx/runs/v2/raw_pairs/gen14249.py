from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") != 10754) | F.col("region").isNull())
    .select("id", "store_name", "sqft")
)
