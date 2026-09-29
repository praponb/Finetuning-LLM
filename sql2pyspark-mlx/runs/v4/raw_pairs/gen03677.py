from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.year("opened_date") >= 2024)
    .select("region", "opened_date")
)
