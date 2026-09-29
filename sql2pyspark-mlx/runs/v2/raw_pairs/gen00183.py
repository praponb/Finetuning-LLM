from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Grab")
    .select("account_id")
    .unionByName(
        df_transactions
        .filter(F.col("account_id").isin([4, 32]))
        .select("account_id")
    )
    .distinct()
    .orderBy("account_id")
)
