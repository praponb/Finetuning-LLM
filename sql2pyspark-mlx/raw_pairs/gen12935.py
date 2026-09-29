from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "IN_TRANSIT", "RETURNED"]))
    .select("cost")
    .union(
        df_payments
        .filter(F.col("method").isin(["card", "cash", "promptpay"]))
        .select("amount")
    )
    .distinct()
    .orderBy("cost")
)
