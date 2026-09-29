from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(1220, 1520))
    .select("id", "status")
    .unionByName(
        df_payments
        .filter(F.col("method").isin(["card", "bank_transfer"]))
        .select("id", "status")
    )
)
