from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_payments
    .filter(F.col("amount").between(450, 1170))
    .select(
        "id",
        "order_id",
        "amount",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
    )
    .orderBy(F.col("amount_share").desc())
)
