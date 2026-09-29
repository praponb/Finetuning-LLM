from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") <= "2023-08-21")
    .select(
        "id",
        "status",
        F.round("shipping_fee", 1).alias("shipping_fee_rounded"),
        F.add_months("order_date", 3).alias("order_date_plus_months"),
    )
    .orderBy("order_date_plus_months")
)
