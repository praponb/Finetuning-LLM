from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["Central", "South", "East"]))
    .select("opened_date", "city")
    .unionByName(
        df_stores
        .filter(F.year("opened_date") == 2025)
        .select("opened_date", "city")
    )
    .distinct()
)
