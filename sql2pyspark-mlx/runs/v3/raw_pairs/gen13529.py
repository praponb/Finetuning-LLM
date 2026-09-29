from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["East", "North", "Central"])
    .agg(F.avg("sqft"))
)
