from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["basic", "standard", "premium"]))
    .select("end_date", F.lit("subscription").alias("source"))
    .unionByName(
        df_products
        .select(F.col("launch_date").alias("end_date"), F.lit("product").alias("source"))
    )
    .orderBy("source")
)
