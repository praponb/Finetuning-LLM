from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["Central", "West"]))
    .select("id", "opened_date", "region")
    .orderBy("opened_date", "id")
    .limit(5)
)
