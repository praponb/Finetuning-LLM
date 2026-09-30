from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "delivered_date",
        "cost",
        F.lead("cost", 1, 0).over(w).alias("next_cost"),
    )
)
