from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(4, 6))
    .select("quantity")
    .subtract(
        df_order_items
        .filter(F.col("unit_price").isNotNull())
        .select("quantity")
    )
    .orderBy("quantity")
)
