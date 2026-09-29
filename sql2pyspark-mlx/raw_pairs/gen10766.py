from pyspark.sql import functions as F

result = (
    df_transactions
    .select("id", "merchant")
    .intersectAll(
        df_transactions
        .filter(F.hour("txn_time").between(10, 15))
        .select("id", "merchant")
    )
)
