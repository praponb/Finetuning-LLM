from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select("id", "channel", "store_id")
    .unionByName(
        df_orders
        .filter(F.month("order_date").isin([4, 5, 10]))
        .select("id", "channel", "store_id")
    )
    .orderBy(F.col("id").desc())
)
