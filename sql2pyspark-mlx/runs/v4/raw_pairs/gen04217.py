from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_first(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

deduped = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "row_num",
        F.when(F.col("status").rlike("^(SUCCESS|FAILED)$"), "GROUP_A").when(F.col("status").rlike("^(REFUNDED)$"), "CORE").otherwise("GROUP_B").alias("status_zone"),
        F.col("avg_shipment_cost"),
    )
    .orderBy("status")
)
