from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(~F.col("o.channel").isin(["mobile", "partner"]))
    .select(
        F.col("o.discount"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.upper(F.col("o.status")).alias("status_upper"),
    )
    .orderBy("opened_date", "city")
)
