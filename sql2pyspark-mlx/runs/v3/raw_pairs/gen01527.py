from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("shipping_fee") > 22).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_anti")
    .filter(F.col("s.opened_date") >= "2023-10-30")
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
    )
)
