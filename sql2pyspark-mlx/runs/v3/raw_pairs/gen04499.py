from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(5, 6))
    .select("id", "quantity")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([19, 21, 37]))
        .select("id", "quantity")
    )
)
