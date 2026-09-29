from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city").isin(["Tokyo", "Singapore", "Bangkok"]))
    .groupBy(F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"))
    .agg(F.min(F.col("i.quantity")).alias("min_quantity"))
    .orderBy(F.col("min_quantity").desc())
)
