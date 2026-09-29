from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Global%"))
    .select("supplier_name", "city")
    .unionByName(
        df_suppliers
        .filter(F.col("rating").between(2.1, 3.2))
        .select("supplier_name", "city")
    )
    .orderBy("city")
)
