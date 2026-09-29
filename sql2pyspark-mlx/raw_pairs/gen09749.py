from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 3)
    .select("id", "customer_id")
    .unionByName(
        df_accounts
        .filter(F.col("status").isin(["OPEN", "FROZEN"]))
        .select("id", "customer_id")
    )
    .orderBy(F.col("id").desc())
)
