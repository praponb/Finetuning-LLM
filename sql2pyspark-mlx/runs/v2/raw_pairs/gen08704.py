from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 3.5) & (F.col("country") == "DE") & F.col("rating").between(2.6, 3.5))
    .select(
        "id",
        "city",
        F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"),
    )
)
