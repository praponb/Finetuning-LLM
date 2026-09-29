from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "city",
        F.concat_ws("-", "segment", "name").alias("segment_name_key"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)
