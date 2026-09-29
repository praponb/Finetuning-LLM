from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["Kerry", "UPS", "Flash"]))
    .select(
        "id",
        "status",
        "shipped_date",
        "cost",
        F.lag("cost", 1).over(w).alias("prev_cost"),
        F.row_number().over(w2).alias("rn"),
    )
)
