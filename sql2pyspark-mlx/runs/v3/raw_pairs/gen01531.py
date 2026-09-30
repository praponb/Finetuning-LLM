from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "card")
    .select("id", "status", "method")
    .unionByName(
        df_payments
        .filter(F.col("amount") >= 620)
        .select("id", "status", "method")
    )
    .orderBy(F.col("status").desc())
)
