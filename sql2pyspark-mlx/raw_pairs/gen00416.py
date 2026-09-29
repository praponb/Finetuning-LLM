from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("O%"))
    .select("store_name")
    .unionByName(
        df_stores
        .filter(F.col("region").isin(["West", "North", "South"]))
        .select("store_name")
    )
    .distinct()
    .orderBy(F.col("store_name").desc())
)
