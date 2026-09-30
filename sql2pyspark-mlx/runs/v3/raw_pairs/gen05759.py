from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%oky%"))
    .select("id", "supplier_name")
    .unionByName(
        df_suppliers
        .filter(~F.col("city").isin(["Bangkok", "New York"]))
        .select("id", "supplier_name")
    )
    .distinct()
)
