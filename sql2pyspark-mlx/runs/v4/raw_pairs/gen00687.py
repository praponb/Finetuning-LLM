from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date") <= "2025-08-13")
        .select("status")
    )
    .orderBy(F.col("status").desc())
)
