from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 140)
    .select("id", "quantity")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price") >= 200)
        .select("id", "quantity")
    )
    .distinct()
)
