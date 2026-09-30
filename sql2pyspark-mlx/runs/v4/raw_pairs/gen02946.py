from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Tokyo", "Singapore"]) | F.col("city").isin(["Berlin", "London"]))
    .select("sqft", "store_name", "region")
)
