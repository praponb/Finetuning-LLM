from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["COMPLETED", "PENDING"]))
    .select(
        "id",
        "discount",
        "store_id",
        "amount",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
    )
    .orderBy(F.col("store_id").desc())
)
