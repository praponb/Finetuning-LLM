from pyspark.sql import functions as F

parsed = (
    df_carts
    .filter(F.col("customer_id") == 1)
    .select(
        "id",
        "customer_id",
        "items",
        "ship_to",
        "attributes",
        F.size("attributes").alias("num_attributes"),
        F.coalesce(F.col("attributes")["gift"], F.lit("none")).alias("gift_or_default"),
    )
)

result = (
    parsed
    .groupBy("customer_id")
    .agg(
        F.min("num_attributes").alias("min_num_attributes"),
        F.min("num_attributes").alias("min_num_attributes_2"),
        F.avg("num_attributes").alias("avg_num_attributes"),
    )
)
