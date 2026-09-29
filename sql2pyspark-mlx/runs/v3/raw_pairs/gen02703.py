from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "city",
        "email",
        "is_active",
        F.upper("segment").alias("segment_upper"),
    )
)
