from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select("merchant")
    .unionByName(
        df_transactions
        .filter(F.to_date("txn_time") >= "2024-04-22")
        .select("merchant")
    )
    .orderBy("merchant")
)
