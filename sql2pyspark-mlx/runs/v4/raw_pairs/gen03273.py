from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("region").isin(["Central", "West", "South"]))
    .select("id", "sqft", "store_name")
    .subtract(
        df_stores
        .filter(F.upper("store_name").like("%IVE%"))
        .select("id", "sqft", "store_name")
    )
)
