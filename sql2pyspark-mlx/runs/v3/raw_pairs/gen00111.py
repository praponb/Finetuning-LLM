from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "status",
        "order_date",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
    )
    .orderBy(F.col("id").desc())
)
