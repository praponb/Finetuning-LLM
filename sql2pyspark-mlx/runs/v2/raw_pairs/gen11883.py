from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        F.round("rating", 1).alias("rating_rounded"),
    )
)
