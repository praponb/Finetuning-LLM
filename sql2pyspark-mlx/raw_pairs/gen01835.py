from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 18)
    .select("order_id", "status")
    .intersect(
        df_payments
        .filter(F.col("amount").between(690, 740))
        .select("order_id", "status")
    )
)
