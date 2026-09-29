from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") != "New York")
    .select("store_name", "city")
    .unionByName(
        df_stores
        .filter(F.col("region").isin(["East", "South", "North"]))
        .select("store_name", "city")
    )
    .orderBy("store_name")
)
