from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([29, 34]))
    .select("id", "quantity", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity").between(4, 8))
        .select("id", "quantity", "order_id")
    )
    .orderBy("order_id")
)
