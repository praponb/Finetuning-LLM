from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNull())
    .select("opened_date", "region", "store_name")
)
