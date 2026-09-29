from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([16, 19, 25, 40]))
    .select("order_id")
    .unionByName(
        df_order_items
        .filter(F.col("order_id").isin([2, 9]))
        .select("order_id")
    )
    .distinct()
)
