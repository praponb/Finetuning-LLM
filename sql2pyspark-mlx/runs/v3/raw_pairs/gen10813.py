from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "SG")
    .select("country", "city")
    .unionByName(
        df_suppliers
        .filter(~F.col("country").isin(["UK", "DE"]))
        .select("country", "city")
    )
    .distinct()
)
