from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "New York")
    .select("city")
    .unionByName(
        df_suppliers
        .filter(F.col("country").isin(["TH", "US", "DE"]))
        .select("city")
    )
)
