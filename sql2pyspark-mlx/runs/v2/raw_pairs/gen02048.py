from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "method",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)
