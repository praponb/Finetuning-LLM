from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Lazada")
    .select("merchant", "account_id")
    .unionByName(
        df_transactions
        .filter(F.col("amount") > 3400)
        .select("merchant", "account_id")
    )
    .distinct()
)
