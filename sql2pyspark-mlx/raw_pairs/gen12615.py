from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("email") >= 8)
    .select(
        "name",
        "is_active",
        F.concat_ws("-", "name", "segment").alias("name_segment_key"),
    )
)
