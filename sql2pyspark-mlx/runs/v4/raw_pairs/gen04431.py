from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") < 12213)
    .select("city", "opened_date", "store_name")
    .orderBy("city")
)
