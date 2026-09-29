from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("paid_at"), "order_id").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .select("id", "paid_at", "order_id", F.row_number().over(w).alias("paid_at_row"))
    .orderBy(F.col("paid_at").desc())
)
