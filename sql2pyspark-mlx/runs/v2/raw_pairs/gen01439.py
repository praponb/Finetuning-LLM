from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "FROZEN"]))
    .select("customer_id", "status")
    .unionByName(
        df_accounts
        .filter(F.month("opened_date").isin([2, 7, 12]))
        .select("customer_id", "status")
    )
    .orderBy(F.col("status").desc())
)
