from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "OPEN")
    .select("id", "status")
    .unionByName(
        df_accounts
        .filter(F.col("customer_id").isNull())
        .select("id", "status")
    )
    .orderBy(F.col("status").desc())
)
