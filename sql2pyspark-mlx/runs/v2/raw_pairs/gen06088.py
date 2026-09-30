from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date")
w2 = Window.partitionBy("carrier").orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        F.dense_rank().over(w).alias("shipped_date_dense_rank"),
        F.lag("cost", 1).over(w2).alias("prev_cost"),
    )
)
