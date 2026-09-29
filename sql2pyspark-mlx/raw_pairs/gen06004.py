from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").asc_nulls_first(), "id")

deduped = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        "row_num",
        F.when(F.col("status").rlike("^(REFUNDED|FAILED)$"), "PRIORITY").when(F.col("status").rlike("^(SUCCESS)$"), "EXTENDED").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("paid_at").desc(), "status")
)
