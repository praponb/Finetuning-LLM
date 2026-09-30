from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id")
w2 = Window.partitionBy("order_id", "status").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(F.col("order_id").isin([5, 21, 27]))
    .select(
        "id",
        "status",
        "delivered_date",
        "cost",
        "order_id",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.row_number().over(w2).alias("row_num"),
    )
)
