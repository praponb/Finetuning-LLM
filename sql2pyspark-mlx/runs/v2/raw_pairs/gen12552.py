from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "discount",
        "order_date",
        "status",
        F.date_format("order_date", "yyyy-MM").alias("order_date_label"),
        F.round("amount", 0).alias("amount_rounded"),
    )
    .orderBy(F.col("status").desc())
)
