from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("cost", "id")
w2 = Window.partitionBy("status").orderBy("cost", "id")
w3 = Window.partitionBy("order_id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

scored = (
    df_shipments
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("cost_bucket", F.ntile(4).over(w2))
    .withColumn("order_id_max_cost", F.max("cost").over(w3))
)

result = (
    scored
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "status",
        "event_seq",
        "cost_bucket",
        F.when(F.col("status").rlike("^(LOST|RETURNED)$"), "GROUP_B").otherwise("CORE").alias("status_zone"),
        F.col("max_payment_amount"),
    )
)
