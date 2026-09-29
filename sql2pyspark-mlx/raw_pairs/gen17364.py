from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").isNotNull())
    .select(
        F.col("signup_date").alias("event_date"),
        F.lit("customer").alias("record_type"),
    )
    .unionByName(
        df_products
        .filter(F.col("launch_date").isNotNull())
        .select(
        F.col("launch_date").alias("event_date"),
        F.lit("product").alias("record_type"),
    )
    )
)
