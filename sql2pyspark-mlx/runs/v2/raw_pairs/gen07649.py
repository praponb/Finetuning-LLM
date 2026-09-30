from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("shipped_date") >= "2023-10-01")
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        F.lead("cost", 2).over(w).alias("next_cost"),
    )
)
