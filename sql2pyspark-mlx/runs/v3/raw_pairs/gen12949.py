from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "rating",
        F.when(F.col("country").isin(["UK", "US"]), "Core").otherwise("Other").alias("country_group"),
    )
    .orderBy(F.col("id").desc(), F.col("rating").desc())
)
