from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("s.region"))
    .orderBy(F.col("id").desc())
)
