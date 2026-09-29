from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNull() & (F.col("supplier_name").like("Sia%") | F.col("city").isNotNull()))
    .groupBy(
        F.when(F.col("country").isin(["UK", "US"]), "tier_1").otherwise("other").alias("country_group"),
    )
    .agg(F.avg("rating").alias("avg_rating"))
    .orderBy(F.col("country_group").desc())
)
