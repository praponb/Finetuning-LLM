from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select("order_id", "quantity")
    .unionByName(
        df_order_items
        .filter(F.col("quantity") < 7)
        .select("order_id", "quantity")
    )
    .distinct()
)
