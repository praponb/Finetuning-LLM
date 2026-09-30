from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("shipping_fee") >= 24).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_semi")
    .filter(F.col("s.opened_date") <= "2023-04-01")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
    .orderBy("region")
)
