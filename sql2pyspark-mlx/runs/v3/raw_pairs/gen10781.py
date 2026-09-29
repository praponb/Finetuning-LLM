from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull())
    .select(
        "id",
        "age",
        "email",
        F.concat_ws("/", "name", "segment").alias("name_segment_key"),
    )
)
