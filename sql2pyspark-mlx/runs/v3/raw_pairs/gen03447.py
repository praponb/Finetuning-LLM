from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "status",
        F.concat("method", F.lit(", "), "status").alias("method_status"),
    )
    .orderBy(F.col("method_status").desc(), "id")
    .limit(50)
)
