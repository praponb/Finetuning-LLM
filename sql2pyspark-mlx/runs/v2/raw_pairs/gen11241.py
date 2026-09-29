from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 2.5)
    .select(
        "id",
        "city",
        F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"),
    )
    .orderBy("supplier_name_normalized")
)
