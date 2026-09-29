from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") <= "2025-04-27")
    .groupBy("region")
    .pivot("city", ["Chiang Mai", "Bangkok", "New York", "London"])
    .agg(F.sum("sqft"))
)
