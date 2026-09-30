from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") > 9955) & (F.col("city") == "New York") & (F.col("opened_date") > "2025-06-01"))
    .orderBy("opened_date")
)
