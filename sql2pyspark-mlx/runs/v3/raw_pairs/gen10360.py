from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        F.trim("method").alias("clean_method"),
        F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"),
    )
    .orderBy("paid_at_label")
)
