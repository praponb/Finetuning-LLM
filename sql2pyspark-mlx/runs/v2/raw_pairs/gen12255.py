from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.opened_date") < "2023-06-01")
    .groupBy(
        F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"),
        F.quarter(F.col("s.opened_date")).alias("opened_quarter"),
    )
    .agg(F.countDistinct(F.col("o.shipping_fee")).alias("unique_stores"))
)
