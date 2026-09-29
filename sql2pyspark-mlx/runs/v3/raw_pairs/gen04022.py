from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() & ~F.col("supplier_name").like("Tok%") & F.col("country").isin(["UK", "SG", "JP"]))
)

summary = (
    src
    .groupBy(
        F.when(F.col("city").isin(["London", "Singapore"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(F.sum("rating").alias("total_rating"))
)

result = summary.filter(F.col("total_rating") < 8)
