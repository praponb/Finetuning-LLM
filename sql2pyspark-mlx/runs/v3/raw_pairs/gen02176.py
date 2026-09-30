from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .filter(F.upper(F.trim(F.col("s.store_name"))) == "OLD TOWN")
    .select(F.col("o.id"), F.col("o.status"), F.col("s.city"), F.col("s.opened_date"))
    .orderBy(F.col("id").desc())
)
