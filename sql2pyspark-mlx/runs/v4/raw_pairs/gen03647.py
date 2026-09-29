from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")

result = (
    df_orders
    .filter(F.col("amount").between(620, 670))
    .select(
        "id",
        "customer_id",
        "amount",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
    )
    .orderBy("id")
)
