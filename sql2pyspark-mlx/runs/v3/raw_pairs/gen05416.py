from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["SHIPPED", "CANCELLED", "COMPLETED"]))
    .select("status")
    .subtract(
        df_payments
        .filter(F.col("amount").between(1250, 1330))
        .select("status")
    )
)
