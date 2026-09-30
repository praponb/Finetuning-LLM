from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "amount",
        "status",
        "method",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)
