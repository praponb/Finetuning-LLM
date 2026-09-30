from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Parts%"))
    .select("id", "country", "city")
    .subtract(
        df_suppliers
        .filter(F.col("country") == "UK")
        .select("id", "country", "city")
    )
)
