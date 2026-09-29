from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 3.6)
    .select("id", "supplier_name", "country")
    .unionByName(
        df_suppliers
        .filter(F.col("rating").between(2.8, 3.5))
        .select("id", "supplier_name", "country")
    )
    .orderBy("country")
)
