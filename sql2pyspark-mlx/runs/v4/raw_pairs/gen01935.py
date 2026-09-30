from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") <= 9839)
    .select("id", "store_name", "city", "opened_date")
)
