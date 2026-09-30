from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .select("method", "status")
    .unionByName(
        df_payments
        .filter(F.col("method") != "cash")
        .select("method", "status")
    )
    .distinct()
    .orderBy("status")
)
