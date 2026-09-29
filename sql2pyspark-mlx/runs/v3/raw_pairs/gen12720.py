from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "order_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
)
