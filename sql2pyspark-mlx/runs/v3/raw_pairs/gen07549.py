from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") <= "2025-07-01")
    .select("id", "opened_date", "store_name")
)
