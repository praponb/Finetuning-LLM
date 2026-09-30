from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~((F.col("paid_at") < "2023-09-01") & F.hour("paid_at").between(11, 17)))
    .select(
        "id",
        "amount",
        "status",
        "method",
        F.to_date("paid_at").alias("paid_at_date"),
    )
)
