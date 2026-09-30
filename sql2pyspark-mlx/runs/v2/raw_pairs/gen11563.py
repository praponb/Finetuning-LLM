from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([5, 11, 17, 37]))
    .select("order_id", "quantity")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price") >= 110)
        .select("order_id", "quantity")
    )
)
