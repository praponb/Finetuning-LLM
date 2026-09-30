from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2024-06-01")
    .select("region")
    .intersect(df_stores.filter(F.year("opened_date") >= 2025).select("region"))
)
