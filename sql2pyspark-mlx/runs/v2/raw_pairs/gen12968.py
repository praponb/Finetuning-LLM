from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.length("supplier_name") < 7)
    .select("city", "country")
    .unionByName(
        df_suppliers
        .filter(F.length("supplier_name") >= 12)
        .select("city", "country")
    )
    .distinct()
)
