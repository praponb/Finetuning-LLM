from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.length("store_name") < 12)
    .select("store_name", "sqft")
)
