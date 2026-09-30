from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "channel").orderBy("discount", "id")

base = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-07-15"))) & (F.col("order_date") < "2023-07-01"))
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.coalesce("shipping_fee", F.lit(0)).alias("shipping_fee_filled"),
    )
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "shipping_fee_filled",
    )
)
