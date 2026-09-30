from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at_day", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy("paid_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id", "method").orderBy("paid_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("method") == "bank_transfer")
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.col("paid_at").cast("date").alias("paid_at_day"),
        F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"),
    )
)

result = (
    cleaned
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("row_seq_2", F.row_number().over(w2))
    .withColumn("last_known_method", F.last("method", ignorenulls=True).over(w3))
    .withColumn("last_known_method_2", F.last("method", ignorenulls=True).over(w4))
)
