from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2023-05-01")
    .select("delivered_date")
    .intersect(
        df_subscriptions
        .filter(~F.col("plan").isin(["standard", "enterprise", "basic"]))
        .select(F.col("end_date").alias("delivered_date"))
    )
    .orderBy(F.col("delivered_date").desc())
)
