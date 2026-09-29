from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([1, 4, 30]))
    .select("id", "order_id")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price") > 260)
        .select("id", "order_id")
    )
    .distinct()
)
