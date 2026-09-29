from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([5, 16]))
    .select("id", "method")
    .unionByName(
        df_payments
        .filter(F.col("status").isin(["SUCCESS", "FAILED"]))
        .select("id", "method")
    )
    .orderBy("id")
)
