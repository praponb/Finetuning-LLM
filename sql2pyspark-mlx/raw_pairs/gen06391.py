from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .intersectAll(
        df_orders
        .filter(F.col("store_id").isNull())
        .select("status")
    )
)
