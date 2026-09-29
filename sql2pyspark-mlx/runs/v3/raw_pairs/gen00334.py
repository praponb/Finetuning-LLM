from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date") >= "2025-07-01")
        .select("status")
    )
    .orderBy("status")
)
