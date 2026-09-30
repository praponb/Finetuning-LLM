from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "rating",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
    )
)
