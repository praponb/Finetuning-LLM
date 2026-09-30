from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id")
w2 = Window.partitionBy("carrier").orderBy("shipped_date", "id")

result = (
    df_shipments
    .filter(F.col("carrier") == "Flash")
    .select(
        "id",
        "delivered_date",
        "carrier",
        "shipped_date",
        "cost",
        F.row_number().over(w).alias("row_num"),
        F.lead("cost", 1).over(w2).alias("next_cost"),
    )
)
