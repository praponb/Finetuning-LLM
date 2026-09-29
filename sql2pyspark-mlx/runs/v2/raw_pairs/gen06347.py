from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Berlin", "Tokyo"]) | (F.year("opened_date") == 2024))
)
