from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "country",
        "rating",
        "supplier_name",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
    )
    .orderBy("rating")
)
