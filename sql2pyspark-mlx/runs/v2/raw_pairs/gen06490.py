from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("A%"))
    .select("opened_date")
    .unionByName(df_stores.filter(F.col("region") != "Central").select("opened_date"))
    .orderBy("opened_date")
)
