from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") < 8)
    .select("id", "quantity")
    .subtract(
        df_order_items
        .filter(F.col("product_id").isin([1, 31, 36]))
        .select("id", "quantity")
    )
    .orderBy("id")
)
