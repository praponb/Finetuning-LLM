from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc())

result = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "status",
        "delivered_date",
        "cost",
        "carrier",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.dense_rank().over(w2).alias("delivered_date_dense_rank"),
    )
)
