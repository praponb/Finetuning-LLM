from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 3.1)
    .select("city")
    .unionByName(
        df_stores
        .filter(F.col("region").isin(["West", "South", "East"]))
        .select("city")
    )
    .distinct()
    .orderBy("city")
)
