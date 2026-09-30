from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([18, 25, 35]))
    .select("start_date")
    .subtract(
        df_shipments
        .filter(F.col("status") == "RETURNED")
        .select("shipped_date")
    )
    .orderBy(F.col("start_date").desc())
)
