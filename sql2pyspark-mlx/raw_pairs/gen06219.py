from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("status") == "SUCCESS") & (F.col("status") == "FAILED")) | F.col("amount").between(1450, 1590))
    .select(
        "id",
        "method",
        "status",
        "amount",
        F.concat("status", F.lit("/"), "method").alias("status_method"),
    )
    .orderBy(F.col("amount").desc(), "id")
    .limit(5)
)
