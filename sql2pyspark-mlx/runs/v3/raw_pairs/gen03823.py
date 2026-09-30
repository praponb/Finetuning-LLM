from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") > "2024-10-27")
    .groupBy("city")
    .pivot("region", ["East", "West", "North", "Central"])
    .agg(F.count("sqft"))
    .orderBy("city")
)
