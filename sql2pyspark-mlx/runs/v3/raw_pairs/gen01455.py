from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") != "Singapore")
    .select("id", "store_name", "city", "opened_date")
)
