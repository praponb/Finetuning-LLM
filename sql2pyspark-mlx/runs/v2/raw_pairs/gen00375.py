from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([8, 11, 24, 33]))
    .select("plan", "start_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date").between("2023-05-07", "2023-06-01"))
        .select("plan", "start_date")
    )
    .orderBy(F.col("plan").desc())
)
