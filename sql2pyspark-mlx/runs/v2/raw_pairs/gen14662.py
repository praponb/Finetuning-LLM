from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 2.4)
    .select("supplier_name", "country")
    .unionByName(
        df_suppliers
        .filter(F.col("city") == "Singapore")
        .select("supplier_name", "country")
    )
)
