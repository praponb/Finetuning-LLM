from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2025-01-16")
    .select("status", "method")
    .unionByName(
        df_payments
        .filter(F.hour("paid_at").between(11, 19))
        .select("status", "method")
    )
    .distinct()
)
