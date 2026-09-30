from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 2.2) | F.col("rating").between(2.0, 4.0))
    .groupBy(
        "country",
        F.when(F.col("country").isin(["JP", "SG"]), "priority").otherwise("other").alias("country_group"),
    )
    .agg(F.sum("rating").alias("total_rating"))
)
