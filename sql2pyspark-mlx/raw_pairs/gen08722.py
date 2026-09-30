from pyspark.sql import functions as F

result = (
    df_payments
    .select("id", "method")
    .subtract(
        df_payments
        .filter(F.col("order_id").isNotNull())
        .select("id", "method")
    )
)
