from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Berlin") & (F.col("city") == "Tokyo"))
    .select("opened_date", "sqft", "city")
)
