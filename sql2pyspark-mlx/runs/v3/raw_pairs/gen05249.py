from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .select("id", "start_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date").between("2023-12-01", "2024-06-01"))
        .select("id", "start_date")
    )
)
