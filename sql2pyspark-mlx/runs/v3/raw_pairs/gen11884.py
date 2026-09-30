from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 660)
    .select(
        "method",
        "status",
        F.upper("method").alias("method_upper"),
        F.initcap(F.trim("method")).alias("method_title"),
    )
    .orderBy(F.col("method").desc())
)
