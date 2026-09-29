from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "New York") & (F.col("rating") < 3.6))
    .select(
        "id",
        "rating",
        "country",
        "supplier_name",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
        F.when(F.col("country").isin(["TH", "DE"]), "Group A").otherwise("Other").alias("country_group"),
    )
    .orderBy(F.col("id").desc())
)
