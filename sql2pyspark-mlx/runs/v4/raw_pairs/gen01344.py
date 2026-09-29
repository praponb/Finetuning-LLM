from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "DE")
    .select("id", "city")
    .unionByName(
        df_suppliers
        .filter(F.col("country").isin(["UK", "DE", "TH"]))
        .select("id", "city")
    )
    .orderBy("id")
)
