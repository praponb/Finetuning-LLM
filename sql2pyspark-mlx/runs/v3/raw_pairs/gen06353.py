from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w2 = Window.orderBy("shipped_date")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.dense_rank().over(w2).alias("shipped_date_dense_rank"),
    )
)
