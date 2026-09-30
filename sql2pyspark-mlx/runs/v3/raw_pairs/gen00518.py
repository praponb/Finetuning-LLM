from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("G%"))
    .select("supplier_name")
    .unionByName(
        df_suppliers
        .filter(F.col("city").isin(["London", "Berlin"]))
        .select("supplier_name")
    )
    .orderBy("supplier_name")
)
