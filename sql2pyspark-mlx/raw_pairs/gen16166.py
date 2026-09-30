from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .groupBy(
        F.when(F.col("country").isin(["TH", "SG"]), "tier_1").otherwise("other").alias("country_group"),
    )
    .agg(F.avg("rating").alias("avg_rating"))
    .filter(F.col("avg_rating") < 4.1)
)
