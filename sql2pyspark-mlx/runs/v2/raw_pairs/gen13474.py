from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Chiang Mai", "Tokyo"]))
    .select("country", "city")
    .unionByName(
        df_suppliers
        .filter(F.col("rating").between(2.3, 3.5))
        .select("country", "city")
    )
)
