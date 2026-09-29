from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNotNull())
    .select("customer_id")
    .unionByName(
        df_accounts
        .filter(~F.col("status").isin(["CLOSED", "FROZEN"]))
        .select("customer_id")
    )
    .distinct()
)
