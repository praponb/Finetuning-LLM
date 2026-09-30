from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([8, 9, 18, 20]))
    .select("account_id", "merchant")
    .unionByName(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("account_id", "merchant")
    )
    .distinct()
)
