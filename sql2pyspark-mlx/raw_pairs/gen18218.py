from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(11, 19))
    .select("status")
    .unionByName(
        df_payments
        .filter(F.col("status").isin(["REFUNDED", "FAILED"]))
        .select("status")
    )
    .distinct()
)
