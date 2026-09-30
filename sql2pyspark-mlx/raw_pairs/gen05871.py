from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "US")
    .select("id", "city")
    .unionByName(
        df_suppliers
        .filter(F.col("city").isin(["Chiang Mai", "Berlin", "Tokyo"]))
        .select("id", "city")
    )
    .distinct()
    .orderBy("city")
)
