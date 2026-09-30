from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") != "FAILED")
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date").between("2023-04-01", "2023-11-01"))
        .select("status")
    )
    .orderBy(F.col("status").desc())
)
