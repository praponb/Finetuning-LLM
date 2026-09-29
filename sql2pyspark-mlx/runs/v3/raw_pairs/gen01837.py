from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Dow%"))
    .select("id", "region", "opened_date", "sqft")
)
