from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") > "2023-10-29")
    .groupBy("region")
    .pivot("city", ["Bangkok", "Berlin", "London"])
    .agg(F.count("sqft"))
)
