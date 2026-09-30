from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["North", "Central"])
    .agg(F.sum("sqft"))
)
