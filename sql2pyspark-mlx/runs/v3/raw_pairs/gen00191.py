from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "SUCCESS", "FAILED"]))
    .select("id", "status", "method")
    .unionByName(
        df_payments
        .filter(F.col("status") == "REFUNDED")
        .select("id", "status", "method")
    )
    .distinct()
    .orderBy("id")
)
