from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([2, 33]))
    .select("id", "quantity", "order_id")
    .unionByName(
        df_order_items
        .filter(F.col("quantity") >= 4)
        .select("id", "quantity", "order_id")
    )
    .distinct()
    .orderBy(F.col("quantity").desc())
)
