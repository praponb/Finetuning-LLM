from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("delivered_date", "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")
w3 = Window.partitionBy("carrier").orderBy("cost", "id")
w4 = Window.orderBy("row_num", "id").rowsBetween(-2, Window.currentRow)

deduped = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

deduped_shipments = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

with_dims = (
    deduped_shipments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.order_date"), F.col("p.channel"), F.col("p.discount"))
)

metrics = (
    with_dims
    .select(
        "*",
        F.ntile(3).over(w3).alias("cost_bucket"),
        F.avg("cost").over(w4).alias("cost_avg_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "carrier",
        F.col("discount").alias("DISCOUNT_VAL"),
        F.col("cost").alias("COST_VAL"),
    )
    .unpivot(
        ids=["id", "carrier"],
        values=["DISCOUNT_VAL", "COST_VAL"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
