from pyspark.sql import functions as F

result = (
    df_order_items
    .select("id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([17, 21, 30, 37]))
        .select("id")
    )
)
