from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(45000, 70000))
    .select("id", "status", "customer_id")
    .subtract(
        df_accounts
        .filter(~F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]))
        .select("id", "status", "customer_id")
    )
    .orderBy(F.col("id").desc())
)
