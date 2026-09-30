from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.opened_date") > "2023-09-01") | (F.lower(F.trim(F.col("s.store_name"))) == "downtown express"))
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("s.sqft"),
    )
)
