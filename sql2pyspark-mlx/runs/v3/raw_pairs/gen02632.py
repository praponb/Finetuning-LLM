from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("supplier_name").like("%Who%"))
    .select("id", "country", "supplier_name")
    .unionByName(
        df_suppliers
        .filter(F.upper("supplier_name").like("%IAM%"))
        .select("id", "country", "supplier_name")
    )
    .orderBy("id")
)
