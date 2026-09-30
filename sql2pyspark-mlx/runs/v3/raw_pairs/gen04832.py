from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        F.initcap(F.trim("supplier_name")).alias("supplier_name_title"),
    )
    .orderBy("id")
    .limit(5)
)
