from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("city").isin(["Tokyo", "Bangkok", "Singapore"]))
    .groupBy("city")
    .pivot("region", ["Central", "North", "West"])
    .agg(F.max("sqft"))
)
