from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(6830, 10575))
    .select("id", "store_name", "opened_date")
)
