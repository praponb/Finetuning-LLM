from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") < 7) & F.col("order_id").isin([16, 23, 30]))
    .select(
        "quantity",
        F.round((F.col("quantity") * 1.1), 2).alias("adjusted_quantity"),
    )
)
