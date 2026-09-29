from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") < "2025-01-01")
    .select("city")
    .distinct()
)
