from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") < 6041) & (F.col("opened_date") <= "2024-06-01"))
    .select("id", "sqft", "store_name", "city")
)
