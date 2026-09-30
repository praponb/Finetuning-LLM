from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([15, 37]))
    .select("id", "product_id", "quantity")
    .unionByName(
        df_order_items
        .filter(F.col("order_id").isin([16, 30]))
        .select("id", "product_id", "quantity")
    )
    .distinct()
)
