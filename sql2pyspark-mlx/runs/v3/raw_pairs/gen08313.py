from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["London", "Berlin"]))
    .select("id", "country", "supplier_name")
    .unionByName(
        df_suppliers
        .filter(F.col("rating") <= 2.8)
        .select("id", "country", "supplier_name")
    )
)
