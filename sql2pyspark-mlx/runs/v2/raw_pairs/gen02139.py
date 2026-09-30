from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .select("method")
    .unionByName(
        df_payments
        .filter(F.col("amount").between(1420, 1430))
        .select("method")
    )
)
