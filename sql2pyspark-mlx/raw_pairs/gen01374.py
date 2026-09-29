from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.month("start_date").isin([5, 7, 10]))
        .select("status")
    )
    .orderBy(F.col("status").desc())
)
