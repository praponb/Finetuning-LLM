from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 990)
    .select("id", "status")
    .unionByName(
        df_payments
        .filter(~F.col("status").isin(["SUCCESS", "REFUNDED"]))
        .select("id", "status")
    )
    .distinct()
    .orderBy("status")
)
