from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2023-06-01")
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        F.lag("cost", 1).over(w).alias("prev_cost"),
    )
    .orderBy("id", "prev_cost")
)
