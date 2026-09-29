from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@company.co"))
    .select(
        "id",
        "is_active",
        "city",
        "country",
        F.when(F.col("segment").isin(["Corporate", "Consumer"]), "Core").otherwise("Other").alias("segment_group"),
    )
)
