from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "shipped_date",
        "cost",
        F.count("*").over(w).alias("status_count"),
        F.lead("cost", 2).over(w2).alias("next_cost"),
    )
)
