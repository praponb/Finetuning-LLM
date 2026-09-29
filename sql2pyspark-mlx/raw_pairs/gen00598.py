from pyspark.sql import functions as F

kept = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") < F.col("s.opened_date")), "left_semi")
)

result = (
    kept
    .select("id", "store_name", "region", "opened_date")
    .orderBy(F.col("id").desc())
)
