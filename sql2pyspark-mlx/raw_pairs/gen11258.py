from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") >= 3)
    .select("id", "quantity", "order_id")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price").between(250, 280))
        .select("id", "quantity", "order_id")
    )
    .distinct()
)
