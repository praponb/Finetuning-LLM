from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") != 4603)
    .select("id", "store_name")
)
