from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "amount",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)
