from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") < "2025-10-01")
    .select("id", "region", "opened_date")
)
