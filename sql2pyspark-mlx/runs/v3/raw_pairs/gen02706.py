from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%LOB%") & (F.upper(F.trim("supplier_name")) == "TOKYO TRADERS") & F.col("supplier_name").isNotNull())
    .select(
        "id",
        "rating",
        "supplier_name",
        "country",
        F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"),
    )
    .orderBy("id")
)
