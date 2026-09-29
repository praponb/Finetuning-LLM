from pyspark.sql import functions as F

result = (
    df_order_items
    .select("id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id") == 26)
        .select("id", "order_id")
    )
)
