from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 12215)
    .select("id", "store_name")
    .subtract(df_stores.filter(F.col("sqft") > 12668).select("id", "store_name"))
)
