from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(10495, 16076))
    .select("opened_date", "sqft", "city", F.upper("region").alias("region_upper"))
)
