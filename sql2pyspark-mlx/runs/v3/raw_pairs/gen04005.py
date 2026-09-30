from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "US") & (F.col("rating") >= 3.7) & F.col("rating").isNull())
    .select(
        "rating",
        "city",
        "supplier_name",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
    )
)
