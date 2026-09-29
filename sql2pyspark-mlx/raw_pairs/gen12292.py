from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("order_date"), "store_id").orderBy("shipping_fee")

result = (
    df_orders
    .select(
        "id",
        "order_date",
        "store_id",
        "shipping_fee",
        F.percent_rank().over(w).alias("shipping_fee_pct_rank"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
