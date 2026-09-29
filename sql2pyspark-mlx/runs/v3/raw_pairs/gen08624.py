from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 2200)
    .select("merchant", "account_id")
    .unionByName(
        df_transactions
        .filter(F.col("amount") <= 3000)
        .select("merchant", "account_id")
    )
    .distinct()
    .orderBy("merchant")
)
