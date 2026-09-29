from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w2 = Window.partitionBy("status").orderBy("shipped_date", "id")
w_recent = Window.partitionBy("carrier", "order_id").orderBy(F.col("cost").desc_nulls_last())
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

scored = (
    df_shipments
    .withColumn("next_cost", F.lead("cost", 1).over(w_part))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("cost_dense_rank", F.dense_rank().over(w_recent))
)

result = (
    scored
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "status",
        "cost_dense_rank",
        F.col("max_shipment_cost"),
    )
    .orderBy(F.col("cost").desc())
)
