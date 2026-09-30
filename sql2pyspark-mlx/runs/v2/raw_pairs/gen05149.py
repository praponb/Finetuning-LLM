from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") == 10370)
    .select("id", "opened_date")
)
