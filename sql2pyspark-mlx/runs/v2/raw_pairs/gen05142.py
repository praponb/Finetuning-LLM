from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "East")
    .select("id", "city", "opened_date", "sqft")
)
