from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "Central")
    .select("store_name", "opened_date")
    .unionByName(
        df_stores
        .filter(F.col("city").isin(["Singapore", "Tokyo"]))
        .select("store_name", "opened_date")
    )
    .distinct()
    .orderBy("store_name")
)
