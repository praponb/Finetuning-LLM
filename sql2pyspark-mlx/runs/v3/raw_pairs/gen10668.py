from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") > 12716) & (F.length("store_name") >= 5))
    .select("id", "store_name", "city", "opened_date")
)
