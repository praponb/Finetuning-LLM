from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Shell")
    .select("id", "account_id")
    .unionByName(
        df_transactions
        .filter(F.col("amount") > 2800)
        .select("id", "account_id")
    )
)
