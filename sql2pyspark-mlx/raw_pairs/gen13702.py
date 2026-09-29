from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("years_since_order_date").desc())
w2 = Window.orderBy(F.col("years_since_order_date").desc_nulls_first())
w3 = Window.partitionBy("status")

base = (
    df_orders
    .filter(F.col("channel").isNotNull() & (F.col("channel").eqNullSafe(F.lit("mobile"))))
    .select(
        "id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        (F.datediff(F.current_date(), "order_date") / 365.25).alias("years_since_order_date"),
        F.add_months("order_date", 3).alias("order_date_plus_months"),
    )
)

result = (
    base
    .select(
        "id",
        "order_date",
        "amount",
        "shipping_fee",
        "order_date_plus_months",
        F.percent_rank().over(w).alias("years_since_order_date_percent_rank"),
        F.dense_rank().over(w2).alias("years_since_order_date_dense_rank"),
        F.corr("shipping_fee", "amount").over(w3).alias("corr_shipping_fee_amount"),
    )
)
