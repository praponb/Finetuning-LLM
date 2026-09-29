from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") > "2025-07-01")
    .select(
        "id",
        "store_id",
        (F.col("amount") * F.col("discount")).alias("amount_times_discount"),
    )
    .orderBy(F.col("id").desc())
)
