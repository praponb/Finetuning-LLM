from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") < 260)
    .select("id", "quantity", "order_id")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price") >= 140)
        .select("id", "quantity", "order_id")
    )
    .orderBy("id")
)
