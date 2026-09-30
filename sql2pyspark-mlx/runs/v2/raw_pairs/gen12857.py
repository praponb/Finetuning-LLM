from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "London") & F.col("rating").between(2.2, 3.2))
    .select(
        "id",
        "supplier_name",
        "city",
        "country",
        F.coalesce("supplier_name", F.lit("Unknown")).alias("supplier_name_filled"),
    )
)
