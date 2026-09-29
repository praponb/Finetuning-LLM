from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Singapore")
    .select("supplier_name", "city")
    .unionByName(
        df_suppliers
        .filter(F.col("country") != "US")
        .select("supplier_name", "city")
    )
)
