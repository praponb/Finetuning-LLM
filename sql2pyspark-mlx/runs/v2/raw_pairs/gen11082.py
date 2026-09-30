from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") >= 12865) & F.col("sqft").between(5148, 7800) & F.col("store_name").like("O%"))
    .select("id", "region")
)
