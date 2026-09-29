from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .select("id", "status")
    .unionByName(
        df_payments
        .filter(F.col("status") != "SUCCESS")
        .select("id", "status")
    )
)
