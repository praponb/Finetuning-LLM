from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([13, 21, 27, 32]))
    .select("id", "quantity")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([7, 14, 27]))
        .select("id", "quantity")
    )
)
