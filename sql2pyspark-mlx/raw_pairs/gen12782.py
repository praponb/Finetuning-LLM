from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_last())

result = (
    df_payments
    .filter(F.col("amount") >= 530)
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.first("method", ignorenulls=True).over(w).alias("first_known_method"),
        F.dense_rank().over(w_part).alias("amount_dense_rank"),
    )
)
