from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") <= "2025-05-01") & (F.col("shipping_fee").between(29, 31) | (F.col("shipping_fee") >= 25)))
    .select("id", "store_id", F.coalesce("discount", F.lit(0)).alias("discount_filled"))
)
