from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 3900)
    .select("account_id")
    .unionByName(
        df_transactions
        .filter(F.col("amount").between(2900, 3500))
        .select("account_id")
    )
    .distinct()
)
