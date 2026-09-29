from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") < 4)
    .select("quantity", "order_id")
    .unionByName(
        df_order_items
        .filter(F.col("quantity") > 5)
        .select("quantity", "order_id")
    )
    .distinct()
    .orderBy("quantity")
)
