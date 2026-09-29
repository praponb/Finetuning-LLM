from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") != "Chiang Mai")
    .select("id", "supplier_name", "city")
    .unionByName(
        df_suppliers
        .filter(F.col("supplier_name").isNull())
        .select("id", "supplier_name", "city")
    )
)
