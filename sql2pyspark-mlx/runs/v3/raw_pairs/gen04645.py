from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "city",
        F.trim("country").alias("clean_country"),
        F.coalesce("city", F.lit("unassigned")).alias("city_filled"),
    )
)
