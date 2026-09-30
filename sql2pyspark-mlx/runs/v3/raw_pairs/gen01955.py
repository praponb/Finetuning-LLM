from pyspark.sql import functions as F

result = (
    df_customers
    .select("country")
    .unionByName(
        df_suppliers
        .filter(F.lower("supplier_name").like("%oky%"))
        .select("country")
    )
    .distinct()
)
