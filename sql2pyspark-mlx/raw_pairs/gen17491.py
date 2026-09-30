from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"))
    .agg(
        (F.max(F.col("o.discount")) - F.min(F.col("o.discount"))).alias("discount_range"),
    )
    .filter(F.col("discount_range") >= 74)
    .orderBy(F.col("discount_range").desc(), F.col("opened_quarter").desc())
    .limit(5)
)
