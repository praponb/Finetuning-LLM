from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.lit("subscription").alias("origin"), "plan")
    .unionByName(
        df_suppliers
        .select(F.lit("supplier").alias("origin"), F.col("country").alias("plan"))
    )
)
