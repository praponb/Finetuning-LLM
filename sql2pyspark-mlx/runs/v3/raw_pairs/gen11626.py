from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("supplier_name").like("%Sup%"))
    .select("supplier_name", "country")
    .unionByName(
        df_suppliers
        .filter(F.col("country") == "SG")
        .select("supplier_name", "country")
    )
    .orderBy("country")
)
