from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        (F.col("amount") * F.col("discount")).alias("amount_times_discount"),
    )
    .orderBy("id")
    .limit(1)
)
