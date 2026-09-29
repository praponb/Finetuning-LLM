from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "CANCELLED") & (F.col("store_id") == 16))
    .select(
        "discount",
        "order_date",
        "store_id",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)
