from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-08-01", "2024-02-01"))
    .select("id", "monthly_fee")
    .orderBy(F.col("id").desc())
)
