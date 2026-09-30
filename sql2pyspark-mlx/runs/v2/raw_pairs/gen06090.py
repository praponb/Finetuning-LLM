from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 220)
    .select("id", "quantity")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price") <= 380)
        .select("id", "quantity")
    )
    .distinct()
    .orderBy("quantity")
)
