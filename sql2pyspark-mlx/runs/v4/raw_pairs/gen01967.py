from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

latest = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        F.when(F.col("status").rlike("^(COMPLETED|RETURNED)$"), "CORE").when(F.col("status").rlike("^(PENDING|CANCELLED)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy(F.col("status").desc_nulls_last())
)
