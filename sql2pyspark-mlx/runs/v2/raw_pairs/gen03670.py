from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("G%"))
    .select("country", "city")
    .unionByName(
        df_suppliers
        .filter(F.col("country") == "TH")
        .select("country", "city")
    )
    .distinct()
)
