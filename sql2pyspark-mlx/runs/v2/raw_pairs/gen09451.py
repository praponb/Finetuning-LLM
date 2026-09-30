from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["East", "West", "North"]) & ((F.col("sqft") == 14630) | F.col("region").isNotNull()))
    .select("city", "sqft", "opened_date")
)
