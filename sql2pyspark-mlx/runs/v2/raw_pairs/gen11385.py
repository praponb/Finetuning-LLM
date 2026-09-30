from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(280, 350))
    .select("id", "product_id", "quantity")
    .unionByName(
        df_order_items
        .filter(F.col("quantity") >= 7)
        .select("id", "product_id", "quantity")
    )
    .distinct()
)
