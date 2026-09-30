from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["New York", "Tokyo", "London"]) | (F.col("sqft") < 11594))
    .select("id", "store_name", "city", "opened_date")
)
