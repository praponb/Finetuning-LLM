from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Downtown%") & F.col("region").isin(["North", "West"]))
    .select("city", "opened_date", "store_name")
)
