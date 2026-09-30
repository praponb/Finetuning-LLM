from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") != "RETURNED")
    .select("id", "channel", "store_id")
    .subtract(
        df_orders
        .filter(F.col("amount").isNotNull())
        .select("id", "channel", "store_id")
    )
)
