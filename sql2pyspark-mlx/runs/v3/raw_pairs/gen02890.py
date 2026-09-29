from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([19, 40]))
    .select("product_id", "quantity")
    .unionByName(
        df_order_items
        .filter(F.col("order_id").isin([18, 27]))
        .select("product_id", "quantity")
    )
    .distinct()
)
