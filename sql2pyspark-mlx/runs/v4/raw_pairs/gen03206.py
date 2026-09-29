from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(5, 6))
    .select("order_id", "quantity")
    .subtract(
        df_order_items
        .filter(F.col("product_id").isNotNull())
        .select("order_id", "quantity")
    )
    .orderBy("order_id")
)
