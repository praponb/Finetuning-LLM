from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_first(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

latest = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "row_num",
        F.when(F.col("method").rlike("^(card|bank_transfer)$"), "GROUP_B").otherwise("GROUP_A").alias("method_zone"),
        F.col("min_shipment_cost"),
    )
    .orderBy(F.col("status").desc())
)
