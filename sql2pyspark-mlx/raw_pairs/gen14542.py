from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%ION%") | (F.col("country") == "JP"))
    .select(
        "rating",
        "country",
        "city",
        F.initcap(F.trim("supplier_name")).alias("supplier_name_title"),
    )
)
