from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("shipping_fee").between(27, 38)).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_semi")
    .filter(F.col("s.opened_date") > "2025-10-02")
    .select(F.col("s.id"), F.col("s.city"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
