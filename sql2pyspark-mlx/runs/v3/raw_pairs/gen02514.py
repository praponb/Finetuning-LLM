from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status").orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "status",
        "shipped_date",
        "cost",
        F.lag("cost", 1).over(w).alias("prev_cost"),
    )
)
