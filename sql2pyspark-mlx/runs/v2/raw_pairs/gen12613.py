from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([5, 10]))
    .select("id", "quantity")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price") > 380)
        .select("id", "quantity")
    )
    .distinct()
    .orderBy("quantity")
)
