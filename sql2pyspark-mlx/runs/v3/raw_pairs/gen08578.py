from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "JP") & F.col("email").isNotNull())
    .select(
        "id",
        "name",
        F.coalesce("name", F.lit("Unknown")).alias("name_filled"),
        F.concat("city", F.lit(" - "), "segment").alias("city_segment"),
    )
    .orderBy(F.col("id").desc())
)
