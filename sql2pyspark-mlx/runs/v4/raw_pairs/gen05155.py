from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "customer_id").orderBy("amount", "id")

deduped = (
    df_orders
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "discount",
        "shipping_fee",
        F.when(F.col("status").rlike("^(RETURNED|SHIPPED)$"), "EXTENDED").when(F.col("status").rlike("^(COMPLETED|PENDING)$"), "GROUP_B").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy("customer_id")
)
