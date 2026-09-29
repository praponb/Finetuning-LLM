from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc())

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 24)
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        F.percent_rank().over(w).alias("shipping_fee_pct_rank"),
    )
)
