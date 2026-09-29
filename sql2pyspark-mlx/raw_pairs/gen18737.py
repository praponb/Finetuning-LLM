from pyspark.sql import functions as F

result = (
    df_order_items
    .select("order_id")
    .intersect(
        df_order_items
        .filter(F.col("unit_price") > 390)
        .select("order_id")
    )
)
