from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["North", "Central", "South"]))
    .select("id", "opened_date")
)
